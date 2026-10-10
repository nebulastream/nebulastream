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

#include <BackpressureChannel.hpp>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ranges>
#include <stop_token>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <folly/Synchronized.h>

#include <ErrorHandling.hpp>

/// Represents the state of the backpressure channel guarded by a mutex and communicated to the listener via the condition variable.
/// The channel is open as long as no holder applies pressure.
struct Channel
{
    struct State
    {
        size_t holders = 0;
        bool destroyed = false;
    };

    folly::Synchronized<State, std::mutex> stateMtx;
    std::condition_variable_any change;
};

/// Shared by all copies of a Backpressure Controller. Destroying it marks all channels as destroyed.
struct BackpressureController::Channels
{
    std::unordered_map<NES::OriginId, std::shared_ptr<Channel>> byOrigin;

    Channels() = default;
    Channels(const Channels&) = delete;
    Channels& operator=(const Channels&) = delete;
    Channels(Channels&&) = delete;
    Channels& operator=(Channels&&) = delete;

    ~Channels()
    {
        for (const auto& channel : byOrigin | std::views::values)
        {
            channel->stateMtx.lock()->destroyed = true;
            channel->change.notify_all();
        }
    }

    Channel& at(const NES::OriginId origin) const
    {
        const auto it = byOrigin.find(origin);
        INVARIANT(it != byOrigin.end(), "No backpressure channel for origin {}", origin);
        return *it->second;
    }
};

BackpressureController::BackpressureController(std::shared_ptr<Channels> channels) : channels{std::move(channels)}
{
}

BackpressureController::BackpressureController(const BackpressureController& other) : channels{other.channels}
{
}

BackpressureController& BackpressureController::operator=(const BackpressureController& other)
{
    if (this != &other)
    {
        releasePressure();
        channels = other.channels;
    }
    return *this;
}

BackpressureController::BackpressureController(BackpressureController&& other) noexcept
    : channels{std::move(other.channels)}, held{std::exchange(other.held, {})}
{
}

BackpressureController& BackpressureController::operator=(BackpressureController&& other) noexcept
{
    if (this != &other)
    {
        releasePressure();
        channels = std::move(other.channels);
        held = std::exchange(other.held, {});
    }
    return *this;
}

BackpressureController::~BackpressureController()
{
    /// The last holder must not release its pressure, as listeners that are still waiting must observe the destroyed channels instead.
    if (channels.use_count() > 1)
    {
        releasePressure();
    }
}

bool BackpressureController::applyPressure()
{
    bool changed = false;
    for (const auto origin : channels->byOrigin | std::views::keys)
    {
        changed |= applyPressure(origin);
    }
    return changed;
}

bool BackpressureController::releasePressure()
{
    bool changed = false;
    /// Copy, as releasing modifies the set of held origins.
    for (const auto origin : std::vector(held.begin(), held.end()))
    {
        changed |= releasePressure(origin);
    }
    return changed;
}

bool BackpressureController::applyPressure(const NES::OriginId origin)
{
    auto& channel = channels->at(origin);
    if (!held.insert(origin).second)
    {
        return false;
    }
    auto state = channel.stateMtx.lock();
    INVARIANT(!state->destroyed, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
    ++state->holders;
    return true;
}

bool BackpressureController::releasePressure(const NES::OriginId origin)
{
    auto& channel = channels->at(origin);
    if (held.erase(origin) == 0)
    {
        return false;
    }
    bool opened = false;
    {
        auto state = channel.stateMtx.lock();
        INVARIANT(!state->destroyed, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
        opened = --state->holders == 0;
    }
    if (opened)
    {
        channel.change.notify_all();
    }
    return true;
}

bool BackpressureController::controls(const NES::OriginId origin) const
{
    return channels->byOrigin.contains(origin);
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    auto state = channel->stateMtx.lock();
    if (state->holders == 0 && !state->destroyed)
    {
        return;
    }

    channel->change.wait(state.as_lock(), stopToken, [&state] { return state->destroyed || state->holders == 0; });

    INVARIANT(!state->destroyed, "Backpressure Controller was destroyed before the BackpressureListener");
}

std::pair<BackpressureController, std::unordered_map<NES::OriginId, BackpressureListener>>
createBackpressureChannels(const std::vector<NES::OriginId>& origins)
{
    auto channels = std::make_shared<BackpressureController::Channels>();
    std::unordered_map<NES::OriginId, BackpressureListener> listeners;
    for (const auto origin : origins)
    {
        const auto channel = std::make_shared<Channel>();
        const bool inserted = channels->byOrigin.emplace(origin, channel).second;
        INVARIANT(inserted, "Duplicate origin {} for backpressure channels", origin);
        listeners.emplace(origin, BackpressureListener{channel});
    }
    return {BackpressureController{std::move(channels)}, std::move(listeners)};
}

std::pair<BackpressureController, BackpressureListener> createBackpressureChannel()
{
    auto [controller, listeners] = createBackpressureChannels({NES::INITIAL<NES::OriginId>});
    return {std::move(controller), std::move(listeners.begin()->second)};
}
