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
    std::unordered_map<GrpcAddr, std::vector<Exception>> errors;
    std::string errorMessage;

public:
    explicit DistributedException(std::unordered_map<GrpcAddr, std::vector<Exception>> errors);

    std::string& what() noexcept { return errorMessage; }

    const std::unordered_map<GrpcAddr, std::vector<Exception>>& details() const { return errors; }

    std::unordered_map<GrpcAddr, std::vector<Exception>>& details() { return errors; }

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
    DistributedQueryId queryId{DistributedQueryId::INVALID};

    /// Reports a distributed query state based on the individual query states. See @DistributedQueryState for more information.
    DistributedQueryState getGlobalQueryState() const;

    /// Reports the encountered exception per worker node. There might be multiple local queries running on a single worker, so there
    /// might be multiple errors per worker.
    std::unordered_map<GrpcAddr, std::vector<Exception>> getExceptions() const;

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

template <typename AddressType>
struct DecomposedLogicalPlan
{
    std::unordered_map<AddressType, std::vector<LogicalPlan>> localPlans;

    explicit DecomposedLogicalPlan(std::unordered_map<AddressType, std::vector<LogicalPlan>> plans) : localPlans{std::move(plans)} { }
};

struct DistributedLogicalPlan
{
    DistributedLogicalPlan(DecomposedLogicalPlan<GrpcAddr>&& plan, LogicalPlan globalPlan)
        : plan(std::move(plan)), globalPlan(std::move(globalPlan))
    {
        PRECONDITION(not this->plan.localPlans.empty(), "Input plan should not be empty");
    }

    /// Subscript operator for accessing plans by its grpc addr
    const std::vector<LogicalPlan>& operator[](const GrpcAddr& grpc) const
    {
        const auto it = plan.localPlans.find(grpc);
        if (it == plan.localPlans.end())
        {
            throw std::out_of_range(fmt::format("No plan found in decomposed plan under addr {}", grpc));
        }
        return it->second;
    }

    std::vector<LogicalPlan>& operator[](const GrpcAddr& connection) { return plan.localPlans.at(connection); }

    size_t size() const
    {
        return std::ranges::fold_left(
            plan.localPlans | std::views::values | std::views::transform(&std::vector<LogicalPlan>::size), 0, std::plus{});
    }

    const LogicalPlan& getGlobalPlan() const { return globalPlan; }

    [[nodiscard]] const DistributedQueryId& getQueryId() const { return queryId; }

    void setQueryId(DistributedQueryId queryId) { this->queryId = std::move(queryId); }

    auto begin() const { return plan.localPlans.begin(); }

    auto end() const { return plan.localPlans.end(); }

private:
    DistributedQueryId queryId{DistributedQueryId::INVALID};
    DecomposedLogicalPlan<GrpcAddr> plan;
    LogicalPlan globalPlan;
};
}

FMT_OSTREAM(NES::DistributedQueryState);
FMT_OSTREAM(NES::DistributedQuery);
