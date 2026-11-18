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
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Util/Pointers.hpp>
#include <absl/functional/any_invocable.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SharedQueryPlan.hpp>
#include <Version.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

class QuerySubmissionBackend
{
public:
    virtual ~QuerySubmissionBackend() = default;
    [[nodiscard]] virtual std::expected<QueryId, Exception> start(LogicalPlan) = 0;
    virtual std::expected<void, Exception> stop(QueryId) = 0;
    /// Injects a reconfiguration marker of ISQD into a running local query. Returns false if the marker has no new event for the query.
    virtual std::expected<bool, Exception> addReconfigurationMarker(QueryId, const ReconfigurationMarker&) = 0;
    [[nodiscard]] virtual std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId) const = 0;
    [[nodiscard]] virtual std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const = 0;
    [[nodiscard]] virtual std::expected<VersionInfo, Exception> version() const = 0;
};

/// std::move_only_function is the C++23 equivalent but is not yet available in libc++19.
using BackendProvider = absl::AnyInvocable<UniquePtr<QuerySubmissionBackend>(const WorkerConfig&)>;

struct QueryManagerState
{
    std::unordered_map<DistributedQueryId, DistributedQuery> queries;
    /// The placement of the started queries, kept for the incremental placement and deployment of ISQP/ISQD.
    std::shared_ptr<GlobalExecutionPlan> globalExecutionPlan = std::make_shared<GlobalExecutionPlan>();
    std::unordered_map<DistributedQueryId, SharedQueryPlan> sharedQueryPlans;
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
    QueryManagerBackends backends;
    SharedPtr<WorkerCatalog> workerCatalog;
    /// Protects the queries and the backends against the concurrent deployments of parallel placement amendments. Calls to the workers
    /// are made without holding it.
    mutable std::recursive_mutex mutex;

    /// Registers the placement of a started query with the global execution plan, if the plan was placed and decomposed.
    void registerPlacement(
        const DistributedQueryId& id,
        const DistributedLogicalPlan& plan,
        const std::unordered_map<Host, std::vector<QueryId>>& localQueries);

public:
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state);
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);
    /// Compiles and starts the distributed query on all assigned workers. Blocks until the query state has advanced past Registered.
    [[nodiscard]] std::expected<DistributedQueryId, std::vector<Exception>> start(const DistributedLogicalPlan& plan);
    std::expected<void, std::vector<Exception>> stop(const DistributedQueryId& query);
    [[nodiscard]] std::expected<DistributedQueryStatusSnapshot, std::vector<Exception>> status(const DistributedQueryId& query) const;
    [[nodiscard]] std::vector<DistributedQueryId> getRunningQueries() const;
    [[nodiscard]] std::vector<DistributedQueryId> queries() const;
    [[nodiscard]] std::expected<DistributedWorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const;
    [[nodiscard]] std::map<Host, std::expected<VersionInfo, Exception>> workerVersions() const;
    [[nodiscard]] std::expected<DistributedQuery, Exception> getQuery(const DistributedQueryId& query) const;

    /// Accessors for the incremental placement and deployment of ISQP/ISQD.
    [[nodiscard]] GlobalExecutionPlan& getGlobalExecutionPlan() { return *state.globalExecutionPlan; }

    /// The shared query plans of the running queries whose placement is known.
    [[nodiscard]] std::unordered_map<DistributedQueryId, SharedQueryPlan>& getSharedQueryPlans() { return state.sharedQueryPlans; }

    /// Starts a local query of a running distributed query on the worker and adds it to the distributed query.
    [[nodiscard]] std::expected<QueryId, Exception> startLocalQuery(const DistributedQueryId& query, const Host& worker, LogicalPlan plan);
    /// Stops a local query and removes it from its distributed query.
    std::expected<void, Exception> stopLocalQuery(const DistributedQueryId& query, const Host& worker, const QueryId& localQuery);
    /// Removes a local query that ended by itself (e.g., it was drained) from its distributed query.
    void removeLocalQuery(const DistributedQueryId& query, const Host& worker, const QueryId& localQuery);
    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> localQueryStatus(const Host& worker, const QueryId& localQuery) const;
    [[nodiscard]] std::expected<bool, Exception>
    addReconfigurationMarker(const Host& worker, const QueryId& localQuery, const ReconfigurationMarker& marker);

    [[nodiscard]] bool hasWorker(const Host& worker) const;
};

}
