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

#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/RecordingLifecycleManager.hpp>
#include <ReplayPlanning.hpp>
#include <Replay/ReplayExecution.hpp>
#include <Replay/ReplayStorage.hpp>
#include <RecordingCatalog.hpp>
#include <Util/Pointers.hpp>
#include <absl/functional/any_invocable.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

class QuerySubmissionBackend
{
public:
    virtual ~QuerySubmissionBackend() = default;
    [[nodiscard]] virtual std::expected<QueryId, Exception>
    registerQuery(
        LogicalPlan,
        std::optional<ReplayCheckpointRegistration> replayCheckpointRegistration = std::nullopt,
        std::optional<ReplayQueryRuntimeControl> replayRuntimeControl = std::nullopt)
        = 0;
    virtual std::expected<void, Exception> start(QueryId) = 0;
    virtual std::expected<void, Exception> stop(QueryId) = 0;
    virtual std::expected<void, Exception> unregister(QueryId) = 0;
    [[nodiscard]] virtual std::expected<LocalQueryStatus, Exception> status(QueryId) const = 0;
    [[nodiscard]] virtual std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const = 0;
    [[nodiscard]] virtual std::expected<std::vector<uint64_t>, Exception>
    selectReplaySegments(const std::string& recordingFilePath, const Replay::BinaryStoreReplaySelection& selection) const = 0;
    [[nodiscard]] virtual std::expected<std::vector<uint64_t>, Exception>
    pinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds) = 0;
    virtual std::expected<void, Exception> unpinReplaySegments(const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds)
        = 0;
};

using BackendProvider = absl::AnyInvocable<UniquePtr<QuerySubmissionBackend>(const WorkerConfig&)>;

struct QueryManagerState
{
    std::unordered_map<DistributedQueryId, DistributedQuery> queries;
    std::unordered_map<ReplayExecutionId, ReplayExecution> replayExecutions;
    std::unordered_map<ReplayExecutionId, ReplayPlan> replayPlans;
    RecordingCatalog recordingCatalog{};
    std::unordered_set<DistributedQueryId> internalQueries{};
    std::unordered_map<DistributedQueryId, DistributedQueryId> activeRecordingEpochQueriesByBeneficiary{};
    std::unordered_map<DistributedQueryId, DistributedQueryId> pendingRecordingEpochQueriesByBeneficiary{};
};

struct QueryRegistrationOptions
{
    bool internal = false;
    bool includeInReplayPlanning = true;
    std::vector<ReplayCheckpointReference> replayCheckpoints = {};
    std::unordered_map<Host, std::vector<std::optional<std::string>>> replayRestoreFingerprintsByHost = {};
    std::optional<ReplayQueryRuntimeControl> replayRuntimeControl = std::nullopt;
};

/// Manages the lifecycle of distributed queries in a NebulaStream cluster.
/// Tracks active queries, maintains query submission backends (embedded or gRPC),
/// and coordinates query registration, execution, and status polling.
/// Thread-safety: Single-threaded, called from CLI/REPL main thread.
/// State model: Queries transition Registered → Running → Stopped/Failed.
class QueryManager
{
    class QueryManagerBackends
    {
        SharedPtr<WorkerCatalog> workerCatalog;
        mutable BackendProvider backendProvider;
        mutable std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>> backends;
        mutable uint64_t cachedWorkerCatalogVersion = 0;
        static std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>>
        createBackends(const std::vector<WorkerConfig>& workers, BackendProvider& provider);
        void rebuildBackendsIfNeeded() const;

    public:
        QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);

        auto begin() const
        {
            rebuildBackendsIfNeeded();
            return backends.begin();
        }

        auto end() const { return backends.end(); }

        bool contains(const Host& worker) const
        {
            rebuildBackendsIfNeeded();
            return backends.contains(worker);
        }

        const QuerySubmissionBackend& at(const Host& worker) const
        {
            rebuildBackendsIfNeeded();
            return *backends.at(worker);
        }

        QuerySubmissionBackend& at(const Host& worker)
        {
            rebuildBackendsIfNeeded();
            return *backends.at(worker);
        }
    };

    QueryManagerState state;
    RecordingLifecycleManager recordingLifecycleManager;
    std::shared_ptr<WorkerCatalog> workerCatalog;
    QueryManagerBackends backends;
    [[nodiscard]] std::expected<DistributedQueryId, Exception>
    registerQueryImpl(
        const DistributedLogicalPlan& plan,
        std::optional<LogicalPlan> originalPlan,
        QueryRegistrationOptions options);
    std::expected<void, std::vector<Exception>> unregisterQueryImpl(const DistributedQueryId& query, bool cascadeRecordingEpochQueries);
    [[nodiscard]] bool recordingEpochQueryReady(const DistributedQueryId& queryId) const;
    void retireRecordingEpochQuery(const DistributedQueryId& queryId);
    void reconcileRecordingEpochQueries();

public:
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state);
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);
    [[nodiscard]] std::expected<DistributedQueryId, Exception>
    registerQuery(
        const DistributedLogicalPlan& plan,
        std::optional<LogicalPlan> originalPlan = std::nullopt,
        QueryRegistrationOptions options = {});
    [[nodiscard]] std::expected<ReplayExecution, Exception>
    registerReplayExecution(const DistributedQueryId& queryId, uint64_t intervalStartMs, uint64_t intervalEndMs);
    [[nodiscard]] std::expected<ReplayExecution, Exception> getReplayExecution(const ReplayExecutionId& replayExecutionId) const;
    [[nodiscard]] std::expected<ReplayPlan, Exception> getReplayPlan(const ReplayExecutionId& replayExecutionId) const;
    [[nodiscard]] std::expected<void, Exception> updateReplayExecution(ReplayExecution replayExecution);
    [[nodiscard]] std::expected<DistributedQueryId, Exception>
    registerRecordingEpochQuery(const DistributedQueryId& beneficiaryQueryId, const DistributedLogicalPlan& plan);
    /// Starts a pre-registered query. Start may potentially block waiting for the query state to change (even if it fails).
    std::expected<void, std::vector<Exception>> start(DistributedQueryId query);
    std::expected<void, std::vector<Exception>> stop(DistributedQueryId query);
    std::expected<void, std::vector<Exception>> unregister(const DistributedQueryId& query);
    [[nodiscard]] std::expected<DistributedQueryStatus, std::vector<Exception>> status(const DistributedQueryId& query) const;
    [[nodiscard]] std::vector<DistributedQueryId> queries() const;
    [[nodiscard]] std::expected<DistributedWorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const;
    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    selectReplaySegments(const Host& host, const std::string& recordingFilePath, const Replay::BinaryStoreReplaySelection& selection) const;
    [[nodiscard]] std::expected<std::vector<uint64_t>, Exception>
    pinReplaySegments(const Host& host, const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds);
    std::expected<void, Exception>
    unpinReplaySegments(const Host& host, const std::string& recordingFilePath, const std::vector<uint64_t>& segmentIds);
    void refreshWorkerMetrics();
    [[nodiscard]] std::expected<DistributedQuery, Exception> getQuery(DistributedQueryId query) const;
    [[nodiscard]] const RecordingCatalog& getRecordingCatalog() const { return state.recordingCatalog; }
    [[nodiscard]] const auto& getReplayExecutions() const { return state.replayExecutions; }
    [[nodiscard]] const auto& getReplayPlans() const { return state.replayPlans; }
    [[nodiscard]] std::vector<DistributedQueryId> getRunningQueries() const;
};

}
