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
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

#include <nlohmann/json.hpp>
#include <yaml-cpp/node/convert.h>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Util/Logger/Formatter.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
using DistributedQueryId = NESStrongType<uint64_t, struct DistributedQueryId_, 0, 1>;

inline DistributedQueryId getNextDistributedQueryId()
{
    static std::atomic_uint64_t id = DistributedQueryId::INITIAL;
    return DistributedQueryId(id++);
}

struct DistributedWorkerStatus
{
    std::unordered_map<GrpcAddr, std::expected<WorkerStatus, Exception>> workerStatus;
};

struct DistributedQueryStatus
{
    std::vector<LocalQueryStatus> localStatusSnapshots;
    DistributedQueryId queryId = INVALID<DistributedQueryId>;

    QueryState getGlobalQueryState() const
    {
        /// Query if considered failed if any local query failed
        if (std::ranges::any_of(localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Failed; }))
        {
            return QueryState::Failed;
        }
        /// Query is not failed, stopped if all local queries have stopped
        if (std::ranges::all_of(localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Stopped; }))
        {
            return QueryState::Stopped;
        }
        /// Query is neither failed nor stopped. Running, if all local queries are running
        if (std::ranges::all_of(localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Running; }))
        {
            return QueryState::Running;
        }
        /// Query is neither failed nor stopped, nor running. Started, if all local queries have started
        if (std::ranges::all_of(localStatusSnapshots, [](const LocalQueryStatus& local) { return local.state == QueryState::Started; }))
        {
            return QueryState::Started;
        }
        /// Some local queries might be stopped, running, or started, but at least one local query has not been started
        return QueryState::Registered;
    }

    std::vector<Exception> getExceptions() const
    {
        std::vector<Exception> exceptions;
        for (const auto& localStatus : localStatusSnapshots)
        {
            if (auto err = localStatus.metrics.error)
            {
                exceptions.push_back(*err);
            }
        }
        return exceptions;
    }

    std::optional<Exception> coalesceException() const
    {
        if (getExceptions().empty())
        {
            return std::nullopt;
        }

        return QueryStatusFailed(fmt::format(
            "Bad Distributed Query State: {}",
            fmt::join(getExceptions() | std::views::transform([](const auto& exception) { return exception.what(); }), ". ")));
    }

    /// Returns the a combined query metrics object:
    /// start: minimum of all local start timestamps
    /// running: minimum of all local running timestamps
    /// stop: maximum of all local stop timestamps
    /// errors: coalesceException
    QueryMetrics coalesceQueryMetrics() const
    {
        QueryMetrics metrics;
        if (std::ranges::all_of(localStatusSnapshots, [](const LocalQueryStatus& local) { return local.metrics.start.has_value(); }))
        {
            for (const auto& localStatus : localStatusSnapshots)
            {
                if (!metrics.start.has_value())
                {
                    metrics.start = localStatus.metrics.start.value();
                }

                if (*metrics.start > localStatus.metrics.start.value())
                {
                    metrics.start = localStatus.metrics.start.value();
                }
            }
        }

        if (std::ranges::all_of(localStatusSnapshots, [](const LocalQueryStatus& local) { return local.metrics.running.has_value(); }))
        {
            for (const auto& localStatus : localStatusSnapshots)
            {
                if (!metrics.running.has_value())
                {
                    metrics.running = localStatus.metrics.running.value();
                }

                if (*metrics.running > localStatus.metrics.running.value())
                {
                    metrics.running = localStatus.metrics.running.value();
                }
            }
        }

        if (std::ranges::all_of(localStatusSnapshots, [](const LocalQueryStatus& local) { return local.metrics.stop.has_value(); }))
        {
            for (const auto& localStatus : localStatusSnapshots)
            {
                if (!metrics.stop.has_value())
                {
                    metrics.stop = localStatus.metrics.stop.value();
                }

                if (*metrics.stop < localStatus.metrics.stop.value())
                {
                    metrics.stop = localStatus.metrics.stop.value();
                }
            }
        }

        metrics.error = coalesceException();
        return metrics;
    }
};

struct LocalQuery
{
    LocalQueryId id = INVALID<LocalQueryId>;
    GrpcAddr grpcAddr;

    LocalQuery(const LocalQueryId id, const GrpcAddr& addr) : id{id}, grpcAddr{addr} { }

    LocalQuery() = default;

    bool operator==(const LocalQuery& other) const = default;
};

class Query
{
    std::vector<LocalQuery> localQueries;

public:
    [[nodiscard]] const std::vector<LocalQuery>& getLocalQueries() const { return localQueries; }

    /// Iteration support - only works in cluster mode
    [[nodiscard]] auto begin() const { return localQueries.begin(); }

    [[nodiscard]] auto end() const { return localQueries.end(); }

    bool operator==(const Query& other) const = default;

    friend std::ostream& operator<<(std::ostream& os, const Query& query);
    Query() = default;

    explicit Query(std::vector<LocalQuery> localQueries) : localQueries(std::move(localQueries)) { }
};

inline std::ostream& operator<<(std::ostream& os, const Query& query)
{
    fmt::print(
        os,
        "Query [{}]",
        fmt::join(
            query.localQueries
                | std::views::transform([](const auto& localQuery) { return fmt::format("{}@{}", localQuery.id, localQuery.grpcAddr); }),
            ", "));
    return os;
}

}

FMT_OSTREAM(NES::Query);
