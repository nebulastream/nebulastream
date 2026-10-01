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

#include <Util/Wait.hpp>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <stop_token>
#include <vector>
#include <poll.h>
#include <unistd.h>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <bits/chrono.h>
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <ErrorHandling.hpp>
#include <scope_guard.hpp>

namespace NES
{

PollResult pollWithStop(
    const std::span<pollfd> descriptors,
    const std::optional<std::chrono::steady_clock::time_point> deadline,
    const std::stop_token& stopToken)
{
    if (stopToken.stop_requested())
    {
        return PollResult::Stopped;
    }

    const auto wakeFd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wakeFd < 0)
    {
        throw UnknownException("Failed to create wait descriptor: {}", getErrorMessageFromERRNO());
    }
    SCOPE_EXIT
    {
        if (::close(wakeFd) != 0)
        {
            NES_WARNING("Failed to close wait descriptor: {}", getErrorMessageFromERRNO());
        }
    };

    const std::stop_callback wakeOnStop(
        stopToken,
        [wakeFd]() noexcept
        {
            constexpr uint64_t wakeValue = 1;
            [[maybe_unused]] const auto result = ::write(wakeFd, &wakeValue, sizeof(wakeValue));
        });

    std::vector<pollfd> polledDescriptors(descriptors.begin(), descriptors.end());
    polledDescriptors.push_back({.fd = wakeFd, .events = POLLIN, .revents = 0});

    while (true)
    {
        auto timeout = -1;
        if (deadline)
        {
            const auto remaining = *deadline - std::chrono::steady_clock::now();
            if (remaining <= std::chrono::steady_clock::duration::zero())
            {
                return PollResult::TimedOut;
            }
            const auto remainingMilliseconds = std::chrono::ceil<std::chrono::milliseconds>(remaining);
            timeout = static_cast<int>(std::min(remainingMilliseconds, std::chrono::milliseconds{std::numeric_limits<int>::max()}).count());
        }

        const auto pollResult = poll(polledDescriptors.data(), polledDescriptors.size(), timeout);
        if (pollResult < 0 && errno == EINTR)
        {
            continue;
        }

        if (stopToken.stop_requested() || (static_cast<uint32_t>(polledDescriptors.back().revents) & static_cast<uint32_t>(POLLIN)) != 0)
        {
            return PollResult::Stopped;
        }
        if (pollResult < 0)
        {
            throw UnknownException("Failed while polling descriptors: {}", getErrorMessageFromERRNO());
        }
        if (pollResult == 0)
        {
            if (deadline && std::chrono::steady_clock::now() < *deadline)
            {
                continue;
            }
            return PollResult::TimedOut;
        }

        for (size_t index = 0; index < descriptors.size(); ++index)
        {
            descriptors[index].revents = polledDescriptors[index].revents;
        }
        return PollResult::Ready;
    }
}

bool waitUntil(const std::chrono::steady_clock::time_point deadline, const std::stop_token& stopToken)
{
    return pollWithStop({}, deadline, stopToken) == PollResult::TimedOut;
}

}
