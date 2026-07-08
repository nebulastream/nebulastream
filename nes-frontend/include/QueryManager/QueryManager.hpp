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
#include <thread>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QuerySupervisor.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
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
    virtual std::expected<void, Exception> terminate(QueryId) = 0;
    [[nodiscard]] virtual std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId) const = 0;
    [[nodiscard]] virtual std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const = 0;
};

/// std::move_only_function is the C++23 equivalent but is not yet available in libc++19.
using BackendProvider = absl::AnyInvocable<UniquePtr<QuerySubmissionBackend>(const WorkerConfig&)>;

struct QueryManagerState
{
    std::unordered_map<DistributedQueryId, folly::Synchronized<DistributedQuery>> queries;
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
        mutable std::mutex mutex;
        static std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>>
        createBackends(const std::vector<WorkerConfig>& workers, BackendProvider& provider);
        void rebuildBackendsIfNeeded() const;

    public:
        QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);

        auto begin() const
        {
            std::lock_guard sl(mutex);
            rebuildBackendsIfNeeded();
            return backends.begin();
        }

        auto end() const
        {
            std::lock_guard sl(mutex);
            return backends.end();
        }

        bool contains(const Host& worker) const
        {
            std::lock_guard sl(mutex);
            rebuildBackendsIfNeeded();
            return backends.contains(worker);
        }

        const QuerySubmissionBackend& at(const Host& worker) const
        {
            std::lock_guard sl(mutex);
            rebuildBackendsIfNeeded();
            return *backends.at(worker);
        }

        QuerySubmissionBackend& at(const Host& worker)
        {
            std::lock_guard sl(mutex);
            rebuildBackendsIfNeeded();
            return *backends.at(worker);
        }

        SharedPtr<WorkerCatalog> getWorkerCatalog()
        {
            std::lock_guard sl(mutex);
            return copyPtr(workerCatalog); // TODO move elsewhere
        }
    };

    QueryManagerState state;
    QueryManagerBackends backends;
    std::unordered_map<DistributedQueryId, QuerySupervisor> supervisors;

    friend class QuerySupervisor;

public:
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state);
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);
    /// Compiles and starts the distributed query on all assigned workers. Blocks until the query state has advanced past Registered.
    [[nodiscard]] std::expected<DistributedQueryId, std::vector<Exception>> start(const DistributedLogicalPlan& plan);
    std::expected<void, std::vector<Exception>> stop(DistributedQueryId query);
    std::expected<void, std::vector<Exception>> superviseNonBlocking(DistributedQueryId distributedQueryId);
    [[nodiscard]] std::expected<DistributedQueryStatusSnapshot, std::vector<Exception>> status(const DistributedQueryId& query);
    [[nodiscard]] std::vector<DistributedQueryId> getRunningQueries();
    [[nodiscard]] std::vector<DistributedQueryId> queries() const;
    [[nodiscard]] std::expected<DistributedWorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const;
    [[nodiscard]] std::expected<folly::Synchronized<DistributedQuery>*, Exception> getQuery(DistributedQueryId query);
};

}
