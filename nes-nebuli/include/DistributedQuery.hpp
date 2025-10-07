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

struct DistributedWorkerStatus
{
    std::unordered_map<GrpcAddr, std::expected<WorkerStatus, Exception>> workerStatus;
};

struct DistributedQueryStatus
{
    std::unordered_map<GrpcAddr, LocalQueryStatus> localStatusSnapshots;
    DistributedQueryId queryId = INVALID<DistributedQueryId>;

    /// Reports a global query state based on the individual query states.
    QueryState getGlobalQueryState() const;

    std::unordered_map<GrpcAddr, Exception> getExceptions() const;

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

struct LocalQuery
{
    LocalQueryId id = INVALID<LocalQueryId>;
    GrpcAddr grpcAddr = GrpcAddr(GrpcAddr::INVALID.value);

    LocalQuery(const LocalQueryId id, const GrpcAddr& addr) : id{id}, grpcAddr{addr} { }

    LocalQuery() = default;

    bool operator==(const LocalQuery& other) const = default;
};

class DistributedQuery
{
    std::vector<LocalQuery> localQueries;

public:
    [[nodiscard]] const std::vector<LocalQuery>& getLocalQueries() const { return localQueries; }

    /// Iteration support - only works in cluster mode
    [[nodiscard]] auto begin() const { return localQueries.begin(); }

    [[nodiscard]] auto end() const { return localQueries.end(); }

    bool operator==(const DistributedQuery& other) const = default;

    friend std::ostream& operator<<(std::ostream& os, const DistributedQuery& query);
    DistributedQuery() = default;

    explicit DistributedQuery(std::vector<LocalQuery> localQueries);
};

std::ostream& operator<<(std::ostream& os, const DistributedQuery& query);

}

FMT_OSTREAM(NES::DistributedQuery);
