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
#include <expected>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

using GrpcAddr = NESStrongStringType<struct GrpcAddr_, "INVALID">;

struct WorkerConfig
{
    GrpcAddr grpc;
};

class QuerySubmissionBackend
{
public:
    virtual ~QuerySubmissionBackend() = default;
    [[nodiscard]] virtual std::expected<QueryId, Exception> registerQuery(LogicalPlan) = 0;
    virtual std::expected<void, Exception> start(QueryId) = 0;
    virtual std::expected<void, Exception> stop(QueryId) = 0;
    virtual std::expected<void, Exception> unregister(QueryId) = 0;
    [[nodiscard]] virtual std::expected<LocalQueryStatus, Exception> status(QueryId) const = 0;
    [[nodiscard]] virtual std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const = 0;
};

struct QueryManagerState
{
    std::unordered_set<QueryId> queries;
};

class QueryManager
{
    QueryManagerState state;
    UniquePtr<QuerySubmissionBackend> backend;

public:
    QueryManager(UniquePtr<QuerySubmissionBackend> backend, QueryManagerState state);
    explicit QueryManager(UniquePtr<QuerySubmissionBackend> backend);
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan);
    /// Starts a pre-registered query. Start may potentially block waiting for the query state to change (even if it fails).
    std::expected<void, Exception> start(QueryId query);
    std::expected<void, Exception> stop(QueryId query);
    std::expected<void, Exception> unregister(QueryId query);
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId query) const;
    [[nodiscard]] std::vector<QueryId> queries() const;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const;
    [[nodiscard]] std::vector<QueryId> getRunningQueries() const;
    [[nodiscard]] std::expected<QueryId, Exception> getQuery(QueryId query) const;
};

}
