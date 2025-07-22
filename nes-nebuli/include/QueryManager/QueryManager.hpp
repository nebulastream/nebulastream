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

#include <future>
#include <Listeners/QueryLog.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include <Thread.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

using HostAddr = NESStrongStringType<struct HostAddr_, "INVALID">;
using GrpcAddr = NESStrongStringType<struct GrpcAddr_, "INVALID">;

struct WorkerConfig
{
    HostAddr host;
    GrpcAddr grpc;
};

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

struct QueryManagerState
{
    std::unordered_set<LocalQueryId> queries;
};

class QueryManager;

struct QueryStatusNotifier
{
    struct Notifier
    {
        size_t waiting;
        std::promise<void> promise;
    };

    std::future<void> waitForQuery(LocalQueryId queryId, QueryState state);
    folly::Synchronized<std::unordered_map<std::tuple<LocalQueryId, QueryState>, std::vector<Notifier>>> notifiers;
    QueryManager& owner;
    Thread thread;

    explicit QueryStatusNotifier(QueryManager& owner);
};

class QueryManager
{
    QueryManagerState state;
    UniquePtr<QuerySubmissionBackend> backend;
    QueryStatusNotifier notifier;

public:
    QueryManager(UniquePtr<QuerySubmissionBackend> provider, QueryManagerState state);
    explicit QueryManager(UniquePtr<QuerySubmissionBackend> provider);
    [[nodiscard]] std::expected<LocalQueryId, Exception> registerQuery(const PlanStage::OptimizedLogicalPlan& plan);
    std::expected<void, Exception> start(LocalQueryId query);
    std::expected<void, Exception> stop(LocalQueryId query);
    std::expected<void, Exception> unregister(LocalQueryId query);
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(LocalQueryId query) const;
    [[nodiscard]] std::vector<LocalQueryId> queries() const;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const;
    std::vector<LocalQueryId> getRunningQueries() const;
    std::expected<LocalQueryId, Exception> getQuery(LocalQueryId query) const;
};

}
