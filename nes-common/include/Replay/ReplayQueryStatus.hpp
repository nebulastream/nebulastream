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

#include <cstdint>
#include <optional>

#include <QueryId.hpp>

namespace NES
{

enum class ReplayQueryPhase : uint8_t
{
    FastForwarding = 0,
    Emitting = 1,
};

struct ReplayQueryRuntimeControl
{
    uint64_t emitStartWatermarkMs = 0;

    [[nodiscard]] bool operator==(const ReplayQueryRuntimeControl& other) const = default;
};

struct ReplayQueryStatus
{
    QueryId queryId = INVALID_QUERY_ID;
    ReplayQueryPhase phase = ReplayQueryPhase::FastForwarding;
    std::optional<uint64_t> lastOutputWatermarkMs;

    [[nodiscard]] bool operator==(const ReplayQueryStatus& other) const = default;
};

}
