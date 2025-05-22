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
    SubmitQuerySystemEvent(QueryId queryId, std::string query) : queryId(queryId), query(std::move(query)) { }

    SubmitQuerySystemEvent() = default;
    QueryId queryId = INVALID<QueryId>;
    std::string query;
};

struct StartQuerySystemEvent : BaseSystemEvent
{
    explicit StartQuerySystemEvent(QueryId queryId) : queryId(queryId) { }

    StartQuerySystemEvent() = default;
    QueryId queryId = INVALID<QueryId>;
};

struct StopQuerySystemEvent : BaseSystemEvent
{
    explicit StopQuerySystemEvent(QueryId queryId) : queryId(queryId) { }

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
