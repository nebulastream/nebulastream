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
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>

namespace NES::Runtime
{
using ChronoClock = std::chrono::system_clock;

struct BaseSystemEvent
{
    BaseSystemEvent() = default;
    ChronoClock::time_point timestamp = ChronoClock::now();
};

struct SubmitQuerySystemEvent : BaseSystemEvent
{
    /// If the query has not specified a throughput or latency, we use a min throughput of 0 and a max latency of infinity
    SubmitQuerySystemEvent(const QueryId queryId, std::string query)
        : SubmitQuerySystemEvent(queryId, std::move(query), 0, std::chrono::microseconds(std::numeric_limits<uint64_t>::max()))
    {
    }
    SubmitQuerySystemEvent(const QueryId queryId, std::string query, const double minThroughput, const std::chrono::microseconds maxLatency)
        : queryId(queryId), query(std::move(query)), minThroughput(minThroughput), maxLatency(maxLatency)
    {
    }
    SubmitQuerySystemEvent() = default;
    QueryId queryId = INVALID<QueryId>;
    std::string query;
    double minThroughput;
    std::chrono::microseconds maxLatency;
};

struct StartQuerySystemEvent : BaseSystemEvent
{
    explicit StartQuerySystemEvent(const QueryId queryId) : queryId(queryId) { }
    StartQuerySystemEvent() = default;
    QueryId queryId = INVALID<QueryId>;
};

struct StopQuerySystemEvent : BaseSystemEvent
{
    explicit StopQuerySystemEvent(const QueryId queryId) : queryId(queryId) { }
    StopQuerySystemEvent() = default;
    QueryId queryId = INVALID<QueryId>;
};

using SystemEvent = std::variant<SubmitQuerySystemEvent, StartQuerySystemEvent, StopQuerySystemEvent>;
static_assert(std::is_default_constructible_v<SystemEvent>, "Events should be default constructible");

struct SystemEventListener
{
    virtual ~SystemEventListener() = default;
    virtual void onEvent(SystemEvent event) = 0;
};
}
