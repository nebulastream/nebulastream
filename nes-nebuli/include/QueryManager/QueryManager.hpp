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

#include <expected>
#include <vector>

#include <Listeners/QueryLog.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>

namespace NES
{

class QuerySubmissionBackend
{
public:
    virtual ~QuerySubmissionBackend() = default;
    [[nodiscard]] virtual std::expected<LocalQueryId, Exception> registerQuery(LogicalPlan) = 0;
    virtual std::expected<void, Exception> start(LocalQueryId) = 0;
    virtual std::expected<void, Exception> stop(LocalQueryId) = 0;
    virtual std::expected<void, Exception> unregister(LocalQueryId) = 0;
    [[nodiscard]] virtual std::expected<LocalQueryStatus, Exception> status(LocalQueryId) const = 0;
    [[nodiscard]] virtual std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const = 0;
};

using BackendProvider = std::function<UniquePtr<QuerySubmissionBackend>(const WorkerConfig&)>;

struct QueryManagerState
{
    std::unordered_map<DistributedQueryId, Query> queries;
};

class WorkerCatalog;

class QueryManager
{
    class QueryManagerBackends
    {
        SharedPtr<WorkerCatalog> workerCatalog;
        BackendProvider backendProvider;
        mutable std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>> backends;
        mutable uint64_t cachedWorkerCatalogVersion = 0;
        static std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>>
        createBackends(const std::vector<WorkerConfig>& workers, const BackendProvider& provider);
        void rebuildBackendsIfNeeded() const;

    public:
        QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);
        auto begin() const
        {
            rebuildBackendsIfNeeded();
            return backends.begin();
        }

        auto end() const { return backends.end(); }

        bool contains(const GrpcAddr& grpc) const
        {
            rebuildBackendsIfNeeded();
            return backends.contains(grpc);
        }

        const QuerySubmissionBackend& at(const GrpcAddr& grpc) const
        {
            rebuildBackendsIfNeeded();
            return *backends.at(grpc);
        }

        QuerySubmissionBackend& at(const GrpcAddr& grpc)
        {
            rebuildBackendsIfNeeded();
            return *backends.at(grpc);
        }
    };

    QueryManagerState state;
    QueryManagerBackends backends;

public:
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state);
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider);
    [[nodiscard]] std::expected<DistributedQueryId, Exception> registerQuery(const PlanStage::DistributedLogicalPlan& plan);
    std::expected<void, std::vector<Exception>> start(DistributedQueryId query);
    std::expected<void, std::vector<Exception>> stop(DistributedQueryId query);
    std::expected<void, std::vector<Exception>> unregister(DistributedQueryId query);
    [[nodiscard]] std::expected<DistributedQueryStatus, std::vector<Exception>> status(DistributedQueryId query) const;
    [[nodiscard]] std::vector<DistributedQueryId> queries() const;
    [[nodiscard]] std::expected<DistributedWorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const;
    std::expected<Query, Exception> getQuery(DistributedQueryId query) const;
    std::vector<DistributedQueryId> getRunningQueries() const;
};

}
