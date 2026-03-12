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
#include <Util/UUID.hpp>
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

ReplayExecutionId uniqueReplayExecutionId(const QueryManagerState& state)
{
    auto uniqueId = ReplayExecutionId(generateUUID());
    while (state.replayExecutions.contains(uniqueId))
    {
        uniqueId = ReplayExecutionId(generateUUID());
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

std::optional<QueryRecordingPlanRewrite>
queryPlanRewriteForMetadata(const RecordingSelectionResult& selectionResult, const DistributedQueryId& queryId)
{
    if (!selectionResult.incomingQueryPlanRewrite.has_value())
    {
        return std::nullopt;
    }

    auto queryPlanRewrite = *selectionResult.incomingQueryPlanRewrite;
    queryPlanRewrite.queryId = queryId.getRawValue();
    return queryPlanRewrite;
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

std::expected<ReplayExecution, Exception> QueryManager::getReplayExecution(const ReplayExecutionId& replayExecutionId) const
{
    if (const auto it = state.replayExecutions.find(replayExecutionId); it != state.replayExecutions.end())
    {
        return it->second;
    }
    return std::unexpected(QueryNotFound("Replay execution {} is not known to the QueryManager", replayExecutionId));
}

std::expected<ReplayPlan, Exception> QueryManager::getReplayPlan(const ReplayExecutionId& replayExecutionId) const
{
    if (const auto it = state.replayPlans.find(replayExecutionId); it != state.replayPlans.end())
    {
        return it->second;
    }
    return std::unexpected(QueryNotFound("Replay plan for execution {} is not known to the QueryManager", replayExecutionId));
}

std::expected<void, Exception> QueryManager::updateReplayExecution(ReplayExecution replayExecution)
{
    if (!state.replayExecutions.contains(replayExecution.id))
    {
        return std::unexpected(QueryNotFound("Replay execution {} is not known to the QueryManager", replayExecution.id));
    }
    state.replayExecutions.insert_or_assign(replayExecution.id, std::move(replayExecution));
    return {};
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
    : state(std::move(state)),
      recordingLifecycleManager(this->state.recordingCatalog),
      workerCatalog(copyPtr(workerCatalog)),
      backends(std::move(workerCatalog), std::move(provider))
{
    recordingLifecycleManager.reconcile();
    reconcileRecordingEpochQueries();
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : recordingLifecycleManager(state.recordingCatalog),
      workerCatalog(copyPtr(workerCatalog)),
      backends(std::move(workerCatalog), std::move(provider))
{
    recordingLifecycleManager.reconcile();
    reconcileRecordingEpochQueries();
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
QueryManager::registerQuery(const DistributedLogicalPlan& plan, std::optional<LogicalPlan> originalPlan, QueryRegistrationOptions options)
{
    return registerQueryImpl(plan, std::move(originalPlan), options);
}

[[nodiscard]] std::expected<ReplayExecution, Exception>
QueryManager::registerReplayExecution(const DistributedQueryId& queryId, const uint64_t intervalStartMs, const uint64_t intervalEndMs)
{
    if (!state.queries.contains(queryId))
    {
        return std::unexpected(QueryNotFound("Query {} is not known to the QueryManager", queryId));
    }
    if (intervalEndMs <= intervalStartMs)
    {
        return std::unexpected(InvalidQuerySyntax(
            "Replay interval end {} must be greater than start {}",
            intervalEndMs,
            intervalStartMs));
    }

    const auto metadataIt = state.recordingCatalog.getQueryMetadata().find(queryId);
    if (metadataIt == state.recordingCatalog.getQueryMetadata().end() || !metadataIt->second.replaySpecification.has_value())
    {
        return std::unexpected(UnsupportedQuery("Replay currently requires a replayable query, but {} is not replayable", queryId));
    }
    const auto replayPlan = ReplayPlanner(workerCatalog).plan(metadataIt->second, state.recordingCatalog, intervalStartMs, intervalEndMs);
    if (!replayPlan)
    {
        return std::unexpected(replayPlan.error());
    }

    auto replayExecution = ReplayExecution{
        .id = uniqueReplayExecutionId(state),
        .queryId = queryId,
        .intervalStartMs = intervalStartMs,
        .intervalEndMs = intervalEndMs,
        .state = ReplayExecutionState::Planned,
        .warmupStartMs = replayPlan->warmupStartMs,
        .selectedCheckpoint = replayPlan->selectedCheckpoint,
        .selectedRecordingIds =
            replayPlan->selectedRecordingIds | std::views::transform([](const auto& recordingId) { return recordingId.getRawValue(); })
            | std::ranges::to<std::vector>(),
        .pinnedSegments = {},
        .internalQueryIds = {},
        .failureReason = std::nullopt};
    state.replayExecutions.insert_or_assign(replayExecution.id, replayExecution);
    state.replayPlans.insert_or_assign(replayExecution.id, *replayPlan);
    return replayExecution;
}

[[nodiscard]] std::expected<DistributedQueryId, Exception>
QueryManager::registerRecordingEpochQuery(const DistributedQueryId& beneficiaryQueryId, const DistributedLogicalPlan& plan)
{
    if (!state.queries.contains(beneficiaryQueryId))
    {
        return std::unexpected(QueryRegistrationFailed("Cannot install a recording epoch for unknown query {}", beneficiaryQueryId));
    }
    if (state.pendingRecordingEpochQueriesByBeneficiary.contains(beneficiaryQueryId))
    {
        return std::unexpected(QueryRegistrationFailed(
            "Cannot install a second pending recording epoch for query {}",
            beneficiaryQueryId));
    }

    auto result = registerQueryImpl(
        plan,
        std::nullopt,
        QueryRegistrationOptions{.internal = true, .includeInReplayPlanning = false});
    if (!result)
    {
        return result;
    }

    state.pendingRecordingEpochQueriesByBeneficiary.insert_or_assign(beneficiaryQueryId, *result);
    reconcileRecordingEpochQueries();
    return result;
}

[[nodiscard]] std::expected<DistributedQueryId, Exception>
QueryManager::registerQueryImpl(
    const DistributedLogicalPlan& plan, std::optional<LogicalPlan> originalPlan, const QueryRegistrationOptions options)
{
    std::unordered_map<Host, std::vector<QueryId>> localQueries;

    auto id = options.internal ? DistributedQueryId(DistributedQueryId::INVALID) : plan.getQueryId();
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
        if (options.replayCheckpoint.has_value()
            && (!options.replayCheckpoint->host.empty() && Host(options.replayCheckpoint->host) == host)
            && localPlans.size() != 1U)
        {
            return std::unexpected(UnsupportedQuery(
                "Replay checkpoint restoration currently requires exactly one local replay plan on host {}, but found {}",
                host,
                localPlans.size()));
        }
        for (auto localPlan : localPlans)
        {
            try
            {
                /// Set the distributed query ID on the local plan before sending to worker
                localPlan.setQueryId(QueryId::createDistributed(id));
                const auto replayCheckpointForHost = options.replayCheckpoint.has_value()
                        && (options.replayCheckpoint->host.empty() || Host(options.replayCheckpoint->host) == host)
                    ? options.replayCheckpoint
                    : std::nullopt;
                const auto result = backends.at(host).registerQuery(localPlan, replayCheckpointForHost);
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
    if (options.internal)
    {
        this->state.internalQueries.insert(id);
    }
    const auto& selectionResult = plan.getRecordingSelectionResult();
    this->state.recordingCatalog.upsertQueryMetadata(
        id,
        ReplayableQueryMetadata{
            .originalPlan = options.includeInReplayPlanning ? std::move(originalPlan) : std::nullopt,
            .globalPlan = options.includeInReplayPlanning ? std::make_optional(plan.getGlobalPlan()) : std::nullopt,
            .replaySpecification = options.includeInReplayPlanning ? plan.getReplaySpecification() : std::nullopt,
            .selectedRecordings = {},
            .networkExplanations = {},
            .queryPlanRewrite = options.includeInReplayPlanning ? queryPlanRewriteForMetadata(selectionResult, id) : std::nullopt,
            .replayBoundaries = options.includeInReplayPlanning ? selectionResult.incomingQueryReplayBoundaries
                                                                : std::vector<QueryRecordingPlanInsertion>{}});

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
                .ownerQueries = ownerQueries,
                .lifecycleState = std::nullopt,
                .retainedStartWatermark = std::nullopt,
                .retainedEndWatermark = std::nullopt,
                .fillWatermark = std::nullopt,
                .segmentCount = std::nullopt,
                .storageBytes = std::nullopt,
                .successorRecordingId = std::nullopt});
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
        recordingLifecycleManager.notePendingActivation(selectionResult.selectedRecordings.back().recordingId);
    }
    recordingLifecycleManager.reconcile();
    reconcileRecordingEpochQueries();
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
    return state.queries | std::views::keys
        | std::views::filter([this](const auto& id) { return !state.internalQueries.contains(id); })
        | std::ranges::to<std::vector>();
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
                        | std::ranges::to<std::unordered_map<std::string, ReplayOperatorStatistics>>(),
                    .recordingStatuses = status.replayMetrics.recordingStatuses,
                    .replayCheckpoints = status.replayMetrics.replayCheckpoints}))
        {
            NES_WARNING("Could not refresh runtime metrics for unknown worker {}", host);
            continue;
        }
        state.recordingCatalog.reconcileWorkerRuntimeStatus(host, status.replayMetrics.recordingStatuses);
    }
    recordingLifecycleManager.reconcile();
    reconcileRecordingEpochQueries();
}

std::vector<DistributedQueryId> QueryManager::getRunningQueries() const
{
    return queries()
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
    return unregisterQueryImpl(queryId, true);
}

std::expected<void, std::vector<Exception>> QueryManager::unregisterQueryImpl(
    const DistributedQueryId& queryId, const bool cascadeRecordingEpochQueries)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();
    std::vector<Exception> exceptions{};
    std::vector<DistributedQueryId> managedRecordingEpochQueries;

    if (cascadeRecordingEpochQueries && !state.internalQueries.contains(queryId))
    {
        if (const auto pendingIt = state.pendingRecordingEpochQueriesByBeneficiary.find(queryId);
            pendingIt != state.pendingRecordingEpochQueriesByBeneficiary.end())
        {
            managedRecordingEpochQueries.push_back(pendingIt->second);
            state.pendingRecordingEpochQueriesByBeneficiary.erase(pendingIt);
        }
        if (const auto activeIt = state.activeRecordingEpochQueriesByBeneficiary.find(queryId);
            activeIt != state.activeRecordingEpochQueriesByBeneficiary.end())
        {
            managedRecordingEpochQueries.push_back(activeIt->second);
            state.activeRecordingEpochQueriesByBeneficiary.erase(activeIt);
        }
    }

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
    auto erased = state.queries.erase(queryId);
    INVARIANT(erased == 1, "Should not unregister query that has not been registered");
    state.internalQueries.erase(queryId);
    std::erase_if(
        state.pendingRecordingEpochQueriesByBeneficiary,
        [&queryId](const auto& beneficiaryAndQuery) { return beneficiaryAndQuery.second == queryId; });
    std::erase_if(
        state.activeRecordingEpochQueriesByBeneficiary,
        [&queryId](const auto& beneficiaryAndQuery) { return beneficiaryAndQuery.second == queryId; });
    recordingLifecycleManager.reconcile();
    reconcileRecordingEpochQueries();

    for (const auto& managedQueryId : managedRecordingEpochQueries)
    {
        if (!state.queries.contains(managedQueryId))
        {
            continue;
        }
        if (auto result = unregisterQueryImpl(managedQueryId, false); !result)
        {
            std::ranges::copy(result.error(), std::back_inserter(exceptions));
        }
    }

    if (!exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    return {};
}

bool QueryManager::recordingEpochQueryReady(const DistributedQueryId& queryId) const
{
    const auto metadataIt = state.recordingCatalog.getQueryMetadata().find(queryId);
    if (metadataIt == state.recordingCatalog.getQueryMetadata().end())
    {
        return false;
    }

    const auto& selectedRecordings = metadataIt->second.selectedRecordings;
    return !selectedRecordings.empty()
        && std::ranges::all_of(
            selectedRecordings,
            [this](const auto& recordingId)
            {
                const auto recording = state.recordingCatalog.getRecording(recordingId);
                return recording.has_value() && recording->lifecycleState == Replay::RecordingLifecycleState::Ready;
            });
}

void QueryManager::retireRecordingEpochQuery(const DistributedQueryId& queryId)
{
    if (!state.queries.contains(queryId))
    {
        return;
    }

    const auto statusResult = status(queryId);
    if (!statusResult)
    {
        NES_WARNING(
            "Could not inspect recording epoch query {} before retirement: {}",
            queryId,
            fmt::join(statusResult.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
    }
    else if (
        statusResult->getGlobalQueryState() == DistributedQueryState::Running
        || statusResult->getGlobalQueryState() == DistributedQueryState::PartiallyStopped)
    {
        if (const auto stopResult = stop(queryId); !stopResult)
        {
            NES_WARNING(
                "Could not stop retired recording epoch query {}: {}",
                queryId,
                fmt::join(stopResult.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
        }
    }

    if (const auto unregisterResult = unregisterQueryImpl(queryId, false); !unregisterResult)
    {
        NES_WARNING(
            "Could not unregister retired recording epoch query {}: {}",
            queryId,
            fmt::join(unregisterResult.error() | std::views::transform([](const auto& exception) { return exception.what(); }), ", "));
    }
}

void QueryManager::reconcileRecordingEpochQueries()
{
    const auto pendingEpochs = state.pendingRecordingEpochQueriesByBeneficiary | std::ranges::to<std::vector>();
    for (const auto& [beneficiaryQueryId, pendingQueryId] : pendingEpochs)
    {
        if (!state.queries.contains(beneficiaryQueryId) || !state.queries.contains(pendingQueryId))
        {
            state.pendingRecordingEpochQueriesByBeneficiary.erase(beneficiaryQueryId);
            continue;
        }
        if (!recordingEpochQueryReady(pendingQueryId))
        {
            continue;
        }

        const auto previousActiveIt = state.activeRecordingEpochQueriesByBeneficiary.find(beneficiaryQueryId);
        const auto previousActiveQueryId = previousActiveIt != state.activeRecordingEpochQueriesByBeneficiary.end()
            ? std::make_optional(previousActiveIt->second)
            : std::nullopt;

        state.activeRecordingEpochQueriesByBeneficiary.insert_or_assign(beneficiaryQueryId, pendingQueryId);
        state.pendingRecordingEpochQueriesByBeneficiary.erase(beneficiaryQueryId);

        if (previousActiveQueryId.has_value() && *previousActiveQueryId != pendingQueryId)
        {
            retireRecordingEpochQuery(*previousActiveQueryId);
        }
    }
}

}
