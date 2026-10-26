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
        throw IOError("Failed to create wait descriptor: {}", getErrorMessageFromERRNO());
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

    const auto pollOnce = [&]
    {
        auto timeout = -1;
        if (deadline)
        {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(*deadline - std::chrono::steady_clock::now());
            timeout = static_cast<int>(
                std::clamp(remaining, std::chrono::milliseconds::zero(), std::chrono::milliseconds{std::numeric_limits<int>::max()})
                    .count());
        }
        return poll(polledDescriptors.data(), polledDescriptors.size(), timeout);
    };

    auto pollResult = pollOnce();
    while ((pollResult < 0 && errno == EINTR) || (pollResult == 0 && deadline && std::chrono::steady_clock::now() < *deadline))
    {
        pollResult = pollOnce();
    }

    /// The wake descriptor is the last entry; poll reports POLLIN on it once the stop callback has written to it.
    /// revents is a bit mask, so POLLIN is tested with a bitwise and rather than equality, as other bits may be set too.
    const auto wokenByStop = (static_cast<uint32_t>(polledDescriptors.back().revents) & static_cast<uint32_t>(POLLIN)) != 0;
    if (stopToken.stop_requested() || wokenByStop)
    {
        return PollResult::Stopped;
    }
    if (pollResult < 0)
    {
        throw IOError("Failed while polling descriptors: {}", getErrorMessageFromERRNO());
    }
    if (pollResult == 0)
    {
        return PollResult::TimedOut;
    }

    for (size_t index = 0; index < descriptors.size(); ++index)
    {
        descriptors[index].revents = polledDescriptors[index].revents;
    }
    return PollResult::Ready;
}

bool waitUntil(const std::chrono::steady_clock::time_point deadline, const std::stop_token& stopToken)
{
    return pollWithStop({}, deadline, stopToken) == PollResult::TimedOut;
}

}
