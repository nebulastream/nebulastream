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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
using DistributedQueryId = NESStrongType<uint64_t, struct DistributedQueryId_, 0, 1>;
DistributedQueryId getNextDistributedQueryId();

enum class DistributedQueryState : uint8_t
{
    Registered, /// all local queries have been registered
    Running, /// all local queries have been started and are currently running. none of them has stopped
    PartiallyStopped, /// all local queries have been started and are currently running, and at least one of them has stopped
    Stopped, /// all local queries have stopped
    Failed, /// at least one local query has failed
};

struct DistributedWorkerStatus
{
    std::unordered_map<GrpcAddr, std::expected<WorkerStatus, Exception>> workerStatus;
};

struct DistributedQueryStatus
{
    /// A Distributed Query is deployed onto multiple Worker nodes, referenced by their GRPC Address.
    /// Each worker may host multiple independent local queries for the same distributed query.
    /// There are two kinds of failures per query that are used here. The Expected may contain an exception
    /// encountered when fetching the query status (i.e., the worker is not reachable). The LocalQueryStatus
    /// may include a failure encountered by the worker node during query processing (i.e., bad input format)
    std::unordered_map<GrpcAddr, std::unordered_map<LocalQueryId, std::expected<LocalQueryStatus, Exception>>> localStatusSnapshots;
    DistributedQueryId queryId = INVALID<DistributedQueryId>;

    /// Reports a distributed query state based on the individual query states. See @DistributedQueryState for more information.
    DistributedQueryState getGlobalQueryState() const;

    /// Reports the encountered exception per worker node. There might be multiple local queries running on a single worker, so there
    /// might be multiple errors per worker.
    std::unordered_map<GrpcAddr, std::vector<Exception>> getExceptions() const;

    /// Combines all exceptions into a single exception. In the case where there are multiple
    /// exceptions, they are grouped into a single DistributedFailure exception which contains
    /// the individual exceptions. If there is just a single exception, the exception is returned
    /// directly.
    std::optional<Exception> coalesceException() const;

    /// Returns the combined query metrics object:
    /// start: minimum of all local start timestamps
    /// running: minimum of all local running timestamps
    /// stop: maximum of all local stop timestamps
    /// errors: coalesceException
    QueryMetrics coalesceQueryMetrics() const;
};

class DistributedQuery
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries;

public:
    [[nodiscard]] auto iterate() const
    {
        return localQueries
            | std::views::transform(
                   [](auto& queriesPerWorker)
                   {
                       return queriesPerWorker.second
                           | std::views::transform(
                                  [&queriesPerWorker](auto& localQueryId) -> decltype(auto)
                                  { return std::tie(queriesPerWorker.first, localQueryId); });
                   })
            | std::views::join;
    }

    const auto& getLocalQueries() { return localQueries; }

    bool operator==(const DistributedQuery& other) const = default;

    friend std::ostream& operator<<(std::ostream& os, const DistributedQuery& query);
    DistributedQuery() = default;

    explicit DistributedQuery(std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries);
};

std::ostream& operator<<(std::ostream& ostream, const DistributedQuery& query);
std::ostream& operator<<(std::ostream& ostream, const DistributedQueryState& status);
std::ostream& operator<<(std::ostream& ostream, const DistributedQueryStatus& status);
}

FMT_OSTREAM(NES::DistributedQueryState);
FMT_OSTREAM(NES::DistributedQuery);
