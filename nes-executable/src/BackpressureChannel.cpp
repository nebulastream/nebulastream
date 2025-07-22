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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <utility>

#include <ErrorHandling.hpp>

/// Represents the state of the backpressure channel guarded by a mutex and communicated to the listener via the condition variable.
/// The channel is initially open.
struct Channel
{
    enum State : uint8_t
    {
        OPEN,
        CLOSED,
        DESTROYED,
    };

    std::mutex guard;
    State state = OPEN;
    std::condition_variable_any change;
    /// Track how many valves use this channel
    std::atomic_size_t valveCount{0};
};

Valve::Valve(std::shared_ptr<Channel> channel) : channel{std::move(channel)}
{
    incValveCount(); /// Increment ref count, current object is a new valve
}

Valve::Valve(const Valve& other) : channel{other.channel}
{
    incValveCount(); /// Increment ref count, current object is a new valve
}

Valve::Valve(Valve&& other) noexcept : channel(std::move(other.channel))
{
    /// No ref count change, taking ownership
}

Valve& Valve::operator=(const Valve& other)
{
    if (this != &other)
    {
        decValveCount(); /// Release the old channel this valve was connected to

        channel = other.channel;
        incValveCount(); /// Acquire the new channel
    }
    return *this;
}

Valve& Valve::operator=(Valve&& other) noexcept
{
    if (this != &other)
    {
        decValveCount();
        channel = std::move(other.channel);
    }
    return *this;
}

Valve::~Valve()
{
    decValveCount();
}

void Valve::incValveCount()
{
    if (channel)
    {
        channel->valveCount.fetch_add(1, std::memory_order_relaxed);
    }
}

/// Notify threads before destruction of the last existing valve so they can unblock.
void Valve::decValveCount()
{
    if (channel)
    {
        if (const size_t count = channel->valveCount.fetch_sub(1, std::memory_order_acq_rel); count == 1)
        {
            {
                const std::scoped_lock lock(channel->guard);
                channel->state = Channel::DESTROYED;
            }
            channel->change.notify_all();
        }
    }
}

bool Valve::applyPressure()
{
    auto old = Channel::CLOSED;
    {
        const std::scoped_lock lock(channel->guard);
        old = channel->state;
        channel->state = Channel::CLOSED;
    }
    return old == Channel::OPEN;
}

bool Valve::releasePressure()
{
    auto old = Channel::OPEN;
    {
        const std::scoped_lock lock(channel->guard);
        old = channel->state;
        channel->state = Channel::OPEN;
    }
    if (old == Channel::CLOSED)
    {
        channel->change.notify_all();
        return true;
    }
    return false;
}

void Ingestion::wait(const std::stop_token& stopToken) const
{
    std::unique_lock lock(channel->guard);
    /// If the channel is open, ingestion can proceed
    if (channel->state == Channel::State::OPEN)
    {
        return;
    }

    bool destroyed = false;
    /// Wait for the channel state to change
    channel->change.wait(
        lock,
        stopToken,
        [&destroyed, this] -> bool
        {
            destroyed = channel->state == Channel::DESTROYED;
            return destroyed || channel->state == Channel::OPEN;
        });

    INVARIANT(!destroyed, "Valve was destroyed before the Ingestion");
}

std::pair<Valve, Ingestion> Backpressure()
{
    const auto channel = std::make_shared<Channel>();
    return {Valve{channel}, Ingestion{channel}};
}
