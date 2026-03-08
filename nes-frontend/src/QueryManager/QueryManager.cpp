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

#include <QueryManager/QueryManager.hpp>

#include <chrono>
#include <cmath>
#include <cstddef>
#include <exception>
#include <optional>
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Replay/ReplayStorage.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <fmt/ranges.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{
DistributedQueryId uniqueDistributedQueryId(const QueryManagerState& state)
{
    auto uniqueId = getNextDistributedQueryId();
    size_t counter = 0;
    while (state.queries.contains(uniqueId))
    {
        uniqueId = DistributedQueryId(getNextDistributedQueryId().getRawValue() + std::to_string(counter++));
    }
    return uniqueId;
}

void appendUniqueRecordingId(std::vector<RecordingId>& recordingIds, const RecordingId& recordingId)
{
    if (!std::ranges::contains(recordingIds, recordingId))
    {
        recordingIds.push_back(recordingId);
    }
}

std::vector<DistributedQueryId> affectedQueriesForSelection(const RecordingSelection& selection, const DistributedQueryId& incomingQueryId)
{
    auto affectedQueries
        = selection.beneficiaryQueries | std::views::transform([](const auto& queryId) { return DistributedQueryId(queryId); })
        | std::ranges::to<std::vector>();
    if (selection.coversIncomingQuery && !std::ranges::contains(affectedQueries, incomingQueryId))
    {
        affectedQueries.push_back(incomingQueryId);
    }
    return affectedQueries;
}

bool shouldPersistRecordingDecision(const RecordingSelectionExplanation& explanation)
{
    if (explanation.decision == RecordingSelectionDecision::ReuseExistingRecording)
    {
        return true;
    }
    return explanation.selection.coversIncomingQuery;
}

std::optional<uint64_t> retentionWindowForSelectionExplanation(
    const RecordingSelectionExplanation& explanation, const DistributedLogicalPlan& plan, const RecordingCatalog& recordingCatalog)
{
    switch (explanation.decision)
    {
        case RecordingSelectionDecision::CreateNewRecording:
        case RecordingSelectionDecision::UpgradeExistingRecording:
            if (explanation.selection.coversIncomingQuery)
            {
                return plan.getReplaySpecification().and_then([](const auto& replaySpecification) { return replaySpecification.retentionWindowMs; });
            }
            break;
        case RecordingSelectionDecision::ReuseExistingRecording:
            break;
    }
    return recordingCatalog.getRecording(explanation.selection.recordingId).and_then([](const auto& recording) { return recording.retentionWindowMs; });
}
}

std::expected<DistributedQuery, Exception> QueryManager::getQuery(DistributedQueryId query) const
{
    const auto it = state.queries.find(query);
    if (it == state.queries.end())
    {
        return std::unexpected(QueryNotFound("Query {} is not known to the QueryManager", query));
    }
    return it->second;
}

std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>>
QueryManager::QueryManagerBackends::createBackends(const std::vector<WorkerConfig>& workers, BackendProvider& provider)
{
    std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>> backends;
    for (const auto& workerConfig : workers)
    {
        backends.emplace(workerConfig.host, provider(workerConfig));
    }
    return backends;
}

QueryManager::QueryManagerBackends::QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : workerCatalog(std::move(workerCatalog)), backendProvider(std::move(provider))
{
    rebuildBackendsIfNeeded();
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state)
    : state(std::move(state)), workerCatalog(copyPtr(workerCatalog)), backends(std::move(workerCatalog), std::move(provider))
{
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : workerCatalog(copyPtr(workerCatalog)), backends(std::move(workerCatalog), std::move(provider))
{
}

void QueryManager::QueryManagerBackends::rebuildBackendsIfNeeded() const
{
    /// Lazily creates/rebuilds query submission backends (embedded or gRPC).
    /// Backends are re-created when the worker catalog version changes
    /// (workers added/removed) to pick up topology updates.
    const auto currentVersion = workerCatalog->getVersion();
    if (currentVersion != cachedWorkerCatalogVersion)
    {
        NES_DEBUG("WorkerCatalog version changed from {} to {}, rebuilding backends", cachedWorkerCatalogVersion, currentVersion);
        backends = createBackends(workerCatalog->getAllWorkers(), backendProvider);
        cachedWorkerCatalogVersion = currentVersion;
    }
}

[[nodiscard]] std::expected<DistributedQueryId, Exception>
QueryManager::registerQuery(const DistributedLogicalPlan& plan, std::optional<LogicalPlan> originalPlan)
{
    std::unordered_map<Host, std::vector<QueryId>> localQueries;

    auto id = plan.getQueryId();
    if (id == DistributedQueryId(DistributedQueryId::INVALID))
    {
        id = uniqueDistributedQueryId(state);
    }
    else if (this->state.queries.contains(plan.getQueryId()))
    {
        throw QueryAlreadyRegistered("{}", plan.getQueryId());
    }

    for (const auto& [host, localPlans] : plan)
    {
        INVARIANT(backends.contains(host), "Plan was assigned to a node ({}) that is not part of the cluster", host);
        for (auto localPlan : localPlans)
        {
            try
            {
                /// Set the distributed query ID on the local plan before sending to worker
                localPlan.setQueryId(QueryId::createDistributed(id));
                const auto result = backends.at(host).registerQuery(localPlan);
                if (result)
                {
                    NES_DEBUG("Registration to node {} was successful.", host);
                    localQueries[host].emplace_back(*result);
                    continue;
                }
                return std::unexpected{result.error()};
            }
            catch (const std::exception& e)
            {
                return std::unexpected{QueryRegistrationFailed("Message from external exception: {}", e.what())};
            }
        }
    }

    this->state.queries.emplace(id, std::move(localQueries));
    const auto& selectionResult = plan.getRecordingSelectionResult();
    this->state.recordingCatalog.upsertQueryMetadata(
        id,
        ReplayableQueryMetadata{
            .originalPlan = std::move(originalPlan),
            .globalPlan = plan.getGlobalPlan(),
            .replaySpecification = plan.getReplaySpecification(),
            .selectedRecordings = {},
            .networkExplanations = {}});

    std::unordered_map<DistributedQueryId, std::vector<RecordingId>> selectedRecordingsByQuery;
    std::unordered_map<DistributedQueryId, std::vector<RecordingSelectionExplanation>> networkExplanationsByQuery;

    for (const auto& explanation : selectionResult.networkExplanations)
    {
        const auto ownerQueries = affectedQueriesForSelection(explanation.selection, id);
        for (const auto& ownerQuery : ownerQueries)
        {
            if (!state.recordingCatalog.getQueryMetadata().contains(ownerQuery))
            {
                continue;
            }
            appendUniqueRecordingId(selectedRecordingsByQuery[ownerQuery], explanation.selection.recordingId);
            networkExplanationsByQuery[ownerQuery].push_back(explanation);
        }

        if (!shouldPersistRecordingDecision(explanation))
        {
            continue;
        }

        this->state.recordingCatalog.upsertRecording(
            RecordingEntry{
                .id = explanation.selection.recordingId,
                .node = explanation.selection.node,
                .filePath = explanation.selection.filePath,
                .structuralFingerprint = explanation.selection.structuralFingerprint,
                .retentionWindowMs = retentionWindowForSelectionExplanation(explanation, plan, this->state.recordingCatalog),
                .representation = explanation.selection.representation,
                .ownerQueries = ownerQueries});
    }

    for (auto& [queryId, selectedRecordings] : selectedRecordingsByQuery)
    {
        auto explanationsIt = networkExplanationsByQuery.find(queryId);
        auto explanations = explanationsIt != networkExplanationsByQuery.end() ? std::move(explanationsIt->second) : std::vector<RecordingSelectionExplanation>{};
        if (explanationsIt != networkExplanationsByQuery.end())
        {
            networkExplanationsByQuery.erase(explanationsIt);
        }
        this->state.recordingCatalog.reconcileQuerySelections(queryId, std::move(selectedRecordings), std::move(explanations));
    }

    if (!selectionResult.selectedRecordings.empty())
    {
        this->state.recordingCatalog.setTimeTravelReadRecording(selectionResult.selectedRecordings.back().recordingId);
    }
    if (const auto latestRecording = state.recordingCatalog.getTimeTravelReadRecording(); latestRecording.has_value())
    {
        Replay::updateTimeTravelReadAlias(latestRecording->filePath);
    }
    return id;
}

std::expected<void, std::vector<Exception>> QueryManager::start(DistributedQueryId queryId)
{
    auto queryResult = getQuery(std::move(queryId));
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();
    std::vector<Exception> exceptions;

    const std::chrono::system_clock::time_point queryStartTimestamp = std::chrono::system_clock::now();
    for (const auto& [host, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            const auto result = backends.at(host).start(localQueryId);
            if (result)
            {
                NES_DEBUG("Starting query {} on node {} was successful.", localQueryId, host);
                continue;
            }

            exceptions.emplace_back(result.error());
        }
        catch (std::exception& e)
        {
            exceptions.emplace_back(QueryStartFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }

    /// Poll all queries until there status has changed to something other than Registered.
    /// We do this so the function can guarantee that the next call to status will guarantee that the query is not in the Registered
    /// State anymore.
    auto waitForStatusChange = query.iterate() | std::ranges::to<std::vector>() | std::ranges::to<std::unordered_set>();
    /// The query is expected to be moved into the started state pretty quickly after lowering, it is very unlikely to even observe
    /// the status not changing immediatly, so a rapid polling interval is appropriate.
    constexpr auto statusPollInterval = std::chrono::milliseconds(10);
    constexpr size_t statusRetries = 15;
    for (size_t i = 0; i < statusRetries; ++i)
    {
        std::erase_if(
            waitForStatusChange,
            [&](const auto& pair)
            {
                auto [wId, localQueryId] = pair;
                const auto result = backends.at(wId).status(localQueryId);
                if (!result)
                {
                    exceptions.emplace_back(QueryStartFailed("Waiting for query state to change: {}", result.error()));
                    return true;
                }

                /// Waiting until the query state changed. Even if the query status changes to failed we consider the start to be successful.
                /// Subsequent status requests will find the query in a failed state.
                return result->state != QueryState::Registered;
            });

        if (waitForStatusChange.empty())
        {
            break;
        }
        std::this_thread::sleep_for(statusPollInterval * std::pow(2, i));
    }

    if (!waitForStatusChange.empty())
    {
        exceptions.emplace_back(QueryStartFailed(
            "Query state did not change for local queries after {} retries: {}",
            statusRetries,
            fmt::join(
                waitForStatusChange
                    | std::views::transform([](const auto& pair) { return fmt::format("{}@{}", std::get<1>(pair), std::get<0>(pair)); }),
                ", ")));
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }

    NES_DEBUG(
        "Query {} started successfully after {}.",
        queryId,
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - queryStartTimestamp));
    return {};
}

std::expected<DistributedQueryStatus, std::vector<Exception>> QueryManager::status(const DistributedQueryId& queryId) const
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();

    std::unordered_map<Host, std::unordered_map<QueryId, std::expected<LocalQueryStatus, Exception>>> localStatusResults;

    for (const auto& [host, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            const auto result = backends.at(host).status(localQueryId);
            localStatusResults[host].emplace(localQueryId, result);
        }
        catch (std::exception& e)
        {
            localStatusResults[host].emplace(
                localQueryId, std::unexpected(QueryStatusFailed("Message from external exception: {} ", e.what())));
        }
    }

    return DistributedQueryStatus{.localStatusSnapshots = localStatusResults, .queryId = queryId};
}

std::vector<DistributedQueryId> QueryManager::queries() const
{
    return state.queries | std::views::keys | std::ranges::to<std::vector>();
}

std::expected<DistributedWorkerStatus, Exception> QueryManager::workerStatus(std::chrono::system_clock::time_point after) const
{
    DistributedWorkerStatus distributedStatus;
    for (const auto& [wId, backend] : backends)
    {
        distributedStatus.workerStatus.try_emplace(wId, backend->workerStatus(after));
    }
    return distributedStatus;
}

void QueryManager::refreshWorkerMetrics()
{
    constexpr auto sinceEpoch = std::chrono::system_clock::time_point(std::chrono::milliseconds(0));
    for (const auto& [host, backend] : backends)
    {
        const auto statusResult = backend->workerStatus(sinceEpoch);
        if (!statusResult)
        {
            NES_WARNING("Could not refresh runtime metrics for worker {}: {}", host, statusResult.error());
            continue;
        }

        const auto& status = statusResult.value();
        if (!workerCatalog->updateWorkerRuntimeMetrics(
                host,
                WorkerRuntimeMetrics{
                    .observedAt = status.until,
                    .recordingStorageBytes = status.replayMetrics.recordingStorageBytes,
                    .recordingFileCount = status.replayMetrics.recordingFileCount,
                    .activeQueryCount = status.replayMetrics.activeQueryCount,
                    .replayReadBytes = status.replayMetrics.replayReadBytes,
                    .replayOperatorStatistics
                    = status.replayMetrics.operatorStatistics
                        | std::views::transform(
                            [](const auto& statistic)
                            {
                                return std::pair{statistic.nodeFingerprint, statistic};
                            })
                        | std::ranges::to<std::unordered_map<std::string, ReplayOperatorStatistics>>()}))
        {
            NES_WARNING("Could not refresh runtime metrics for unknown worker {}", host);
        }
    }
}

std::vector<DistributedQueryId> QueryManager::getRunningQueries() const
{
    return state.queries | std::views::keys
        | std::views::transform(
               [this](const auto& id) -> std::optional<std::pair<DistributedQueryId, DistributedQueryStatus>>
               {
                   auto result = status(id);
                   if (result)
                   {
                       return std::optional<std::pair<DistributedQueryId, DistributedQueryStatus>>{{id, *result}};
                   }
                   return std::nullopt;
               })
        | std::views::filter([](const auto& idAndStatus) { return idAndStatus.has_value(); })
        | std::views::filter([](auto idAndStatus) { return idAndStatus->second.getGlobalQueryState() == DistributedQueryState::Running; })
        | std::views::transform([](auto idAndStatus) { return idAndStatus->first; }) | std::ranges::to<std::vector>();
}

std::expected<void, std::vector<Exception>> QueryManager::stop(DistributedQueryId queryId)
{
    auto queryResult = getQuery(std::move(queryId));
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();

    std::vector<Exception> exceptions{};

    for (const auto& [host, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            auto result = backends.at(host).stop(localQueryId);
            if (result)
            {
                NES_DEBUG("Stopping query {} on node {} was successful.", localQueryId, host);
                continue;
            }
            exceptions.push_back(result.error());
        }
        catch (std::exception& e)
        {
            exceptions.push_back(QueryStopFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    return {};
}

std::expected<void, std::vector<Exception>> QueryManager::unregister(const DistributedQueryId& queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();
    std::vector<Exception> exceptions{};

    for (const auto& [host, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            auto result = backends.at(host).unregister(localQueryId);
            if (result)
            {
                NES_DEBUG("Unregister of query {} on node {} was successful.", localQueryId, host);
                continue;
            }
            exceptions.push_back(result.error());
        }
        catch (std::exception& e)
        {
            exceptions.push_back(QueryUnregistrationFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    state.recordingCatalog.removeQueryMetadata(queryId);
    if (const auto latestRecording = state.recordingCatalog.getTimeTravelReadRecording(); latestRecording.has_value())
    {
        Replay::updateTimeTravelReadAlias(latestRecording->filePath);
    }
    else
    {
        Replay::clearTimeTravelReadAlias();
    }
    auto erased = state.queries.erase(queryId);
    INVARIANT(erased == 1, "Should not unregister query that has not been registered");
    return {};
}

}
