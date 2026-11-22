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
#include <expected>
#include <string_view>
#include <utility>
#include <ErrorHandling.hpp>

namespace NES
{

/// Validate before narrowing milliseconds, converting to clock ticks, or adding to the current time.
/// Zero is allowed; functions whose options must be positive should reject it themselves.
[[nodiscard]] inline std::expected<std::chrono::steady_clock::time_point, Exception>
checkedSteadyDeadline(const uint64_t millis, const std::string_view operation, const std::chrono::steady_clock::time_point now)
{
    using Clock = std::chrono::steady_clock;
    using Milliseconds = std::chrono::milliseconds;
    auto available = Clock::duration::max();
    if (now.time_since_epoch() > Clock::duration::zero())
    {
        available -= now.time_since_epoch();
    }
    const auto maxMillis = std::chrono::duration_cast<Milliseconds>(available).count();
    if (!std::in_range<Milliseconds::rep>(millis) || std::cmp_greater(millis, maxMillis))
    {
        return std::unexpected(ArithmeticalError("{} duration {} ms exceeds the representable steady-clock deadline", operation, millis));
    }
    return now + std::chrono::duration_cast<Clock::duration>(Milliseconds{static_cast<Milliseconds::rep>(millis)});
}

[[nodiscard]] inline std::expected<std::chrono::steady_clock::time_point, Exception>
checkedSteadyDeadline(const uint64_t millis, const std::string_view operation)
{
    return checkedSteadyDeadline(millis, operation, std::chrono::steady_clock::now());
}

}
