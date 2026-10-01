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
#include <span>
#include <stop_token>
#include <poll.h>
#include <sys/poll.h>

namespace NES
{

enum class PollResult : uint8_t
{
    Ready,
    TimedOut,
    Stopped,
};

/// Polls caller-owned descriptors until one becomes ready, the optional deadline is reached, or stop is requested.
/// The descriptor revents are updated when PollResult::Ready is returned.
PollResult pollWithStop(
    std::span<pollfd> descriptors, std::optional<std::chrono::steady_clock::time_point> deadline, const std::stop_token& stopToken);

/// Waits until deadline or until stopToken is requested.
/// @return true when the deadline was reached, false when the wait was stopped.
bool waitUntil(std::chrono::steady_clock::time_point deadline, const std::stop_token& stopToken);

}
