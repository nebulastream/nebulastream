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

#include <QuerySubmitter.hpp>

#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>

#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <ErrorHandling.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

QuerySubmitter::QuerySubmitter(std::unique_ptr<QueryManager> queryManager) : queryManager(std::move(queryManager))
{
}

std::expected<DistributedQueryId, Exception> QuerySubmitter::registerQuery(const PlanStage::DistributedLogicalPlan& plan)
{
    /// Make sure the queryplan is passed through serialization logic.
    std::unordered_map<GrpcAddr, std::vector<std::string>> serializationErrorsPerWorker;
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

    if (serializationErrorsPerWorker.empty())
    {
        return queryManager->registerQuery(plan);
    }

    const auto exception = CannotSerialize("Encountered serialization errors: {}", serializationErrorsPerWorker);
    return std::unexpected(exception);
}

void QuerySubmitter::startQuery(DistributedQueryId query)
{
    if (auto started = queryManager->start(query); !started.has_value())
    {
        throw started.error();
    }
    ids.emplace(query);
}

void QuerySubmitter::stopQuery(const DistributedQueryId query)
{
    if (auto stopped = queryManager->stop(query); !stopped.has_value())
    {
        throw stopped.error();
    }
}

void QuerySubmitter::unregisterQuery(const DistributedQueryId query)
{
    if (auto unregistered = queryManager->unregister(query); !unregistered.has_value())
    {
        throw unregistered.error();
    }
}

DistributedQueryStatus QuerySubmitter::waitForQueryTermination(const DistributedQueryId query)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        if (const auto queryStatus = queryManager->status(query))
        {
            if (queryStatus.has_value())
            {
                if (queryStatus->getGlobalQueryState() == QueryState::Stopped)
                {
                    return *queryStatus;
                }
            }
            else
            {
                throw TestException(
                    "Could not get query state: {}",
                    fmt::join(queryStatus.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
            }
        }
    }
}

std::vector<DistributedQueryStatus> QuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<std::pair<NES::DistributedQueryId, DistributedQueryStatus>> results;
        for (const auto id : ids)
        {
            if (auto queryStatus = queryManager->status(id))
            {
                if (queryStatus.has_value())
                {
                    if (queryStatus->getGlobalQueryState() == QueryState::Failed
                        || queryStatus->getGlobalQueryState() == QueryState::Stopped)
                    {
                        results.emplace_back(id, std::move(*queryStatus));
                    }
                }
                else
                {
                    throw TestException(
                        "Could not get query state: {}",
                        fmt::join(
                            queryStatus.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
                }
            }
        }
        if (results.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            continue;
        }

        for (auto& id : results | std::views::keys)
        {
            ids.erase(id);
        }

        return results | std::views::values | std::ranges::to<std::vector>();
    }
}
}
