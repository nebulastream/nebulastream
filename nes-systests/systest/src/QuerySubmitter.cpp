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
#include <QueryStatus.hpp>

namespace NES::Systest
{

namespace
{
constexpr auto POLL_INTERVAL = std::chrono::milliseconds(25);

/// Fabricate a snapshot that looks like a single Failed local query whose error is a timeout.
/// The runner's normal failure reporting path picks it up via getGlobalQueryStatus()/coalesceException().
DistributedQueryStatusSnapshot makeTimeoutSnapshot(const DistributedQueryId& query, std::chrono::milliseconds elapsed)
{
    auto exception = TestException("Systest query {} timed out after {}ms", query, elapsed.count());
    LocalQueryStatusSnapshot local{};
    local.queryId = INVALID_QUERY_ID;
    local.state = QueryStatus::Failed;
    local.metrics.error = exception;

    DistributedQueryStatusSnapshot snapshot;
    snapshot.queryId = query;
    snapshot.localStatusSnapshots[Host("systest")].emplace(INVALID_QUERY_ID, std::move(local));
    return snapshot;
}
}

QuerySubmitter::QuerySubmitter(std::unique_ptr<QueryManager> queryManager, std::chrono::milliseconds queryTimeout)
    : queryManager(std::move(queryManager)), queryTimeout(queryTimeout)
{
}

std::expected<DistributedQueryId, Exception> QuerySubmitter::registerQuery(const DistributedLogicalPlan& plan)
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

    if (serializationErrorsPerWorker.empty())
    {
        return queryManager->registerQuery(plan);
    }

    const auto exception = CannotSerialize("Encountered serialization errors: {}", serializationErrorsPerWorker);
    return std::unexpected(exception);
}

void QuerySubmitter::startQuery(const DistributedQueryId& query)
{
    if (auto started = queryManager->start(query); !started.has_value())
    {
        throw std::move(started.error().at(0));
    }
    startedAt.emplace(query, std::chrono::steady_clock::now());
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
    const auto deadline = std::chrono::steady_clock::now() + queryTimeout;
    while (true)
    {
        std::this_thread::sleep_for(POLL_INTERVAL);
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
        if (std::chrono::steady_clock::now() >= deadline)
        {
            /// Best effort: try to stop the runaway query; ignore any failure since we're already in a failure path.
            (void)queryManager->stop(query);
            return makeTimeoutSnapshot(query, queryTimeout);
        }
    }
}

std::vector<DistributedQueryStatusSnapshot> QuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<std::pair<NES::DistributedQueryId, DistributedQueryStatusSnapshot>> results;
        const auto now = std::chrono::steady_clock::now();
        for (const auto& [id, startTime] : startedAt)
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
                results.emplace_back(id, std::move(*queryStatus));
                continue;
            }

            if (now - startTime >= queryTimeout)
            {
                /// Best effort: kick the worker to stop the query so resources are released.
                /// Ignore any error since we already report the timeout as a failure below.
                (void)queryManager->stop(id);
                results.emplace_back(id, makeTimeoutSnapshot(id, queryTimeout));
            }
        }
        if (results.empty())
        {
            std::this_thread::sleep_for(POLL_INTERVAL);
            continue;
        }

        for (auto& id : results | std::views::keys)
        {
            startedAt.erase(id);
        }

        return results | std::views::values | std::ranges::to<std::vector>();
    }
}
}
