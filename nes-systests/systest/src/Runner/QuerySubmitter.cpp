/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Runner/QuerySubmitter.hpp>

#include <chrono>
#include <memory>
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

QuerySubmitter::QuerySubmitter(std::unique_ptr<QueryManager> queryManager, const std::chrono::milliseconds timeout)
    : queryManager(std::move(queryManager)), timeout(timeout)
{
}

std::expected<DistributedQueryId, Exception> QuerySubmitter::startQuery(const DistributedLogicalPlan& plan)
{
    /// Make sure the queryplan is passed through serialization logic.
    std::unordered_map<Host, std::vector<std::string>> serializationErrorsPerWorker;
    for (const auto& [grpc, localPlans] : plan)
    {
        for (const auto& localPlan : localPlans)
        {
            const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(localPlan);
            const auto deserialized = QueryPlanSerializationUtil::deserializeQueryPlan(serialized);
            if (deserialized != localPlan)
            {
                serializationErrorsPerWorker[grpc].emplace_back(fmt::format(
                    "Query plan serialization is wrong: plan != deserialize(serialize(plan)), with plan:\n{} and "
                    "deserialize(serialize(plan)):\n{}",
                    explain(localPlan, ExplainVerbosity::Debug),
                    explain(deserialized, ExplainVerbosity::Debug)));
            }
        }
    }

    if (!serializationErrorsPerWorker.empty())
    {
        return std::unexpected(CannotSerialize("Encountered serialization errors: {}", serializationErrorsPerWorker));
    }

    auto result = queryManager->start(plan);


    if (!result.has_value())
    {
        return std::unexpected(std::move(result.error().at(0)));
    }

    running.emplace(*result, std::chrono::steady_clock::now());
    return result.value();
}

void QuerySubmitter::stopQuery(const DistributedQueryId& query)
{
    if (auto stopped = queryManager->stop(query); !stopped.has_value())
    {
        throw std::move(stopped.error().at(0));
    }
}

DistributedQueryStatusSnapshot QuerySubmitter::waitForQueryTermination(const DistributedQueryId& query)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        auto queryStatus = queryManager->status(query);
        if (!queryStatus.has_value())
        {
            throw TestException(
                "Could not get query state: {}",
                fmt::join(queryStatus.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
        }
        if (queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Stopped
            || queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Failed)
        {
            return *queryStatus;
        }
    }
}

std::vector<FinishedQuery> QuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<FinishedQuery> results;
        const auto now = std::chrono::steady_clock::now();
        for (const auto& [id, startedAt] : running)
        {
            auto queryStatus = queryManager->status(id);
            if (!queryStatus.has_value())
            {
                throw TestException(
                    "Could not get query state: {}",
                    fmt::join(queryStatus.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
            }
            if (queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Stopped
                || queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Failed)
            {
                results.push_back(FinishedQuery{.id = id, .outcome = std::move(*queryStatus)});
                continue;
            }
            if (timeout > std::chrono::milliseconds{0} and now - startedAt >= timeout)
            {
                auto message = fmt::format("query {} did not reach a terminal state within {} ms", id.getRawValue(), timeout.count());
                /// The query is answered as timed out either way. A stop that fails only adds to the message, because there
                /// is nothing more to do about it here.
                if (auto stopped = queryManager->stop(id); not stopped.has_value())
                {
                    message += fmt::format(
                        ", and stopping it failed: {}",
                        fmt::join(stopped.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
                }
                results.push_back(FinishedQuery{.id = id, .outcome = std::unexpected{QueryWaitTimeout("{}", message)}});
            }
        }
        if (results.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            continue;
        }

        for (const auto& finished : results)
        {
            running.erase(finished.id);
        }
        return results;
    }
}
}
