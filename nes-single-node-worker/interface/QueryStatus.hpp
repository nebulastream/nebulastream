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
#include <optional>
#include <ostream>
#include <Util/Logger/Formatter.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>

namespace NES
{
enum class QueryStatus : uint8_t
{
    Registered,
    Started,
    Running, /// Deployed->Running when calling start()
    Stopped, /// Running->Stopped when calling stop() and in Running state
    Failed,
};

inline std::ostream& operator<<(std::ostream& ostream, const QueryStatus& status)
{
    return ostream << magic_enum::enum_name(status);
}

/// Per-query task counters aggregated from the engine's statistic event stream.
/// Populated by the SingleNodeWorker from its MetricsListener; QueryLog itself leaves them zero.
struct QueryCounters
{
    /// Tuples that entered pipelines, summed across all pipelines. A repeated task re-delivers its buffer and counts again.
    uint64_t processedTuples = 0;
    /// Tasks whose pipeline execution completed successfully.
    uint64_t processedTasks = 0;
    uint64_t expiredTasks = 0;
};

struct QueryMetrics
{
    std::optional<std::chrono::system_clock::time_point> start;
    std::optional<std::chrono::system_clock::time_point> running;
    std::optional<std::chrono::system_clock::time_point> stop;
    std::optional<Exception> error;
    QueryCounters counters;
};

/// Summary structure of the query log for a query
struct LocalQueryStatusSnapshot
{
    QueryId queryId = INVALID_QUERY_ID;
    QueryStatus state = QueryStatus::Registered;
    QueryMetrics metrics{};
};
}

FMT_OSTREAM(NES::QueryStatus);
