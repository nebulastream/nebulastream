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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <ostream>
#include <ranges>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
using DistributedQueryId = NESStrongStringType<struct DistributedQueryId_, "invalid">;
DistributedQueryId getNextDistributedQueryId();

class DistributedException : public std::exception
{
    std::unordered_map<WorkerId, std::vector<Exception>> errors;
    std::string errorMessage;

public:
    explicit DistributedException(std::unordered_map<WorkerId, std::vector<Exception>> errors);

    std::string& what() noexcept { return errorMessage; }

    const std::unordered_map<WorkerId, std::vector<Exception>>& details() const { return errors; }

    std::unordered_map<WorkerId, std::vector<Exception>>& details() { return errors; }

    [[nodiscard]] const char* what() const noexcept override { return errorMessage.c_str(); }

    friend std::ostream& operator<<(std::ostream& os, const Exception& ex);
};

struct DistributedQueryMetrics
{
    std::optional<std::chrono::system_clock::time_point> start;
    std::optional<std::chrono::system_clock::time_point> running;
    std::optional<std::chrono::system_clock::time_point> stop;
    std::optional<DistributedException> error;
};

/// Computed (not stored) global state of a distributed query, derived from individual
/// local query statuses across all workers. Determined by getGlobalQueryState() on each status poll.
enum class DistributedQueryState : uint8_t
{
    Registered, /// all local queries have been registered
    Running, /// all local queries have been started and are currently running. none of them has stopped
    PartiallyStopped, /// all local queries have been started and are currently running, and at least one of them has stopped
    Stopped, /// all local queries have stopped
    Failed, /// at least one local query has failed
    Unreachable, /// at least one worker has not responded
};

struct DistributedWorkerStatus
{
    std::unordered_map<WorkerId, std::expected<WorkerStatus, Exception>> workerStatus;
};

struct DistributedQueryStatus
{
    /// A Distributed Query is deployed onto multiple Worker nodes, referenced by their WorkerId.
    /// Each worker may host multiple independent local queries for the same distributed query.
    /// There are two kinds of failures per query that are used here. The Expected may contain an exception
    /// encountered when fetching the query status (i.e., the worker is not reachable). The LocalQueryStatus
    /// may include a failure encountered by the worker node during query processing (e.g., bad input format)
    std::unordered_map<WorkerId, std::unordered_map<QueryId, std::expected<LocalQueryStatus, Exception>>> localStatusSnapshots;
    DistributedQueryId queryId{DistributedQueryId::INVALID};

    /// Reports a distributed query state based on the individual query states. See @DistributedQueryState for more information.
    DistributedQueryState getGlobalQueryState() const;

    /// Reports the encountered exception per worker node. There might be multiple local queries running on a single worker, so there
    /// might be multiple errors per worker.
    std::unordered_map<WorkerId, std::vector<Exception>> getExceptions() const;

    /// Combines all exceptions into a single exception. In the case where there are multiple
    /// exceptions, they are grouped into a single DistributedFailure exception which contains
    /// the individual exceptions. If there is just a single exception, the exception is returned
    /// directly.
    std::optional<DistributedException> coalesceException() const;

    /// Returns the combined query metrics object:
    /// start: minimum of all local start timestamps
    /// running: minimum of all local running timestamps
    /// stop: maximum of all local stop timestamps
    /// errors: coalesceException
    DistributedQueryMetrics coalesceQueryMetrics() const;
};

class DistributedQuery
{
    std::unordered_map<WorkerId, std::vector<QueryId>> localQueries;

public:
    [[nodiscard]] auto iterate() const
    {
        return localQueries
            | std::views::transform(
                   [](auto& queriesPerWorker)
                   {
                       return queriesPerWorker.second
                           | std::views::transform(
                                  [&queriesPerWorker](auto& queryId) -> decltype(auto)
                                  { return std::tie(queriesPerWorker.first, queryId); });
                   })
            | std::views::join;
    }

    const auto& getLocalQueries() { return localQueries; }

    bool operator==(const DistributedQuery& other) const = default;

    friend std::ostream& operator<<(std::ostream& os, const DistributedQuery& query);
    DistributedQuery() = default;

    explicit DistributedQuery(std::unordered_map<WorkerId, std::vector<QueryId>> localQueries);
};

std::ostream& operator<<(std::ostream& ostream, const DistributedQuery& query);
std::ostream& operator<<(std::ostream& ostream, const DistributedQueryState& status);

template <typename AddressType>
struct DecomposedLogicalPlan
{
    std::unordered_map<AddressType, std::vector<LogicalPlan>> localPlans;

    explicit DecomposedLogicalPlan(std::unordered_map<AddressType, std::vector<LogicalPlan>> plans) : localPlans{std::move(plans)} { }
};

class DistributedLogicalPlan
{
public:
    DistributedLogicalPlan(DecomposedLogicalPlan<WorkerId>&& decomposedPlan, LogicalPlan globalPlan)
        : queryId(globalPlan.getQueryId().getGlobalQueryId()), decomposedPlan(std::move(decomposedPlan)), globalPlan(std::move(globalPlan))
    {
        PRECONDITION(not this->decomposedPlan.localPlans.empty(), "Input plan should not be empty");
    }

    /// Subscript operator for accessing plans by worker id
    const std::vector<LogicalPlan>& operator[](const WorkerId& worker) const
    {
        if (const auto it = decomposedPlan.localPlans.find(worker); it != decomposedPlan.localPlans.end())
        {
            return it->second;
        }
        throw std::out_of_range(fmt::format("No plan found in decomposed plan under worker {}", worker));
    }

    std::vector<LogicalPlan>& operator[](const WorkerId& worker) { return decomposedPlan.localPlans.at(worker); }

    size_t size() const
    {
        return std::ranges::fold_left(
            decomposedPlan.localPlans | std::views::values | std::views::transform(&std::vector<LogicalPlan>::size), 0, std::plus{});
    }

    const LogicalPlan& getGlobalPlan() const { return globalPlan; }

    [[nodiscard]] const DistributedQueryId& getQueryId() const { return queryId; }

    void setQueryId(DistributedQueryId queryId) { this->queryId = std::move(queryId); }

    auto begin() const { return decomposedPlan.localPlans.begin(); }

    auto end() const { return decomposedPlan.localPlans.end(); }

private:
    DistributedQueryId queryId{DistributedQueryId::INVALID};
    DecomposedLogicalPlan<WorkerId> decomposedPlan;
    LogicalPlan globalPlan;
};
}

FMT_OSTREAM(NES::DistributedQueryState);
FMT_OSTREAM(NES::DistributedQuery);
