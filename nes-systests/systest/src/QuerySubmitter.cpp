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
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <GrpcSystemStatsReader.hpp>

namespace NES::Systest
{

QuerySubmitter::QuerySubmitter(
    std::unique_ptr<QueryManager> queryManager, std::shared_ptr<GrpcSystemStatsReader> statsReader, bool eventDrivenTermination)
    : queryManager(std::move(queryManager)), statsReader(std::move(statsReader)), useEventDrivenTermination(eventDrivenTermination)
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
        auto result = queryManager->registerQuery(plan);
        if (result.has_value())
        {
            /// Build reverse mapping: LocalQueryId → DistributedQueryId
            /// so the socket reader can map events back.
            auto queryResult = queryManager->getQuery(*result);
            if (queryResult.has_value())
            {
                for (const auto& [host, localQueryId] : queryResult->iterate())
                {
                    localToDistributed.insert_or_assign(localQueryId.getLocalQueryId().getRawValue(), *result);
                }
            }
        }
        return result;
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
    ids.emplace(query);
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

std::vector<DistributedQueryStatusSnapshot> QuerySubmitter::finishedQueries()
{
    if (statsReader && useEventDrivenTermination)
    {
        return finishedQueriesBySocket();
    }
    return finishedQueriesByPolling();
}

CompilationStats QuerySubmitter::getCompilationStats(const DistributedQueryId& query) const
{
    if (!statsReader)
    {
        return {};
    }

    CompilationStats aggregated;
    for (const auto& [localId, distId] : localToDistributed)
    {
        if (distId == query)
        {
            auto stats = statsReader->getCompilationStats(localId);
            aggregated.totalCompileTimeNs += stats.totalCompileTimeNs;
            aggregated.pipelineCount += stats.pipelineCount;
        }
    }
    return aggregated;
}

IngestionStats QuerySubmitter::getIngestionStats(const DistributedQueryId& query) const
{
    if (!statsReader)
    {
        return {};
    }

    IngestionStats aggregated;
    for (const auto& [localId, distId] : localToDistributed)
    {
        if (distId == query)
        {
            auto stats = statsReader->getIngestionStats(localId);
            aggregated.totalTuples += stats.totalTuples;
            aggregated.totalBuffers += stats.totalBuffers;
            aggregated.windowCount += stats.windowCount;
        }
    }
    return aggregated;
}

std::vector<DistributedQueryStatusSnapshot> QuerySubmitter::finishedQueriesByPolling()
{
    while (true)
    {
        std::vector<std::pair<NES::DistributedQueryId, DistributedQueryStatusSnapshot>> results;
        for (const auto& id : ids)
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

std::vector<DistributedQueryStatusSnapshot> QuerySubmitter::finishedQueriesBySocket()
{
    while (true)
    {
        auto events = statsReader->waitForTerminalEvents();
        if (events.empty())
        {
            /// Reader was stopped — fall back to polling for any remaining queries
            return finishedQueriesByPolling();
        }

        /// Map socket events to DistributedQueryIds and fetch full status snapshots.
        /// Deduplicate: in multi-node topologies, each local sub-query emits its own event,
        /// but we only report each DistributedQueryId once.
        std::unordered_map<DistributedQueryId, DistributedQueryStatusSnapshot> resultMap;
        for (const auto& event : events)
        {
            auto it = localToDistributed.find(event.queryId);
            if (it == localToDistributed.end())
            {
                /// Event for a query we don't track (e.g., a system query) — skip
                continue;
            }

            const auto& distributedId = it->second;
            if (!ids.contains(distributedId) || resultMap.contains(distributedId))
            {
                continue;
            }

            /// Fetch the full status snapshot via the QueryManager (single call, not polling)
            auto queryStatus = queryManager->status(distributedId);
            if (!queryStatus.has_value())
            {
                NES_WARNING(
                    "Could not get status for query {} after socket event: {}",
                    distributedId,
                    fmt::join(queryStatus.error() | std::views::transform([](const auto& e) { return e.what(); }), ", "));
                continue;
            }

            /// Only report if truly terminal (the query might have multiple local sub-queries)
            if (queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Stopped
                || queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Failed)
            {
                resultMap.emplace(distributedId, std::move(*queryStatus));
            }
        }

        std::vector<std::pair<DistributedQueryId, DistributedQueryStatusSnapshot>> results(resultMap.begin(), resultMap.end());

        if (results.empty())
        {
            /// Events were for non-tracked queries, keep waiting
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
