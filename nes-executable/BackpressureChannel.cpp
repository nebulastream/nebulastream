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

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>

struct Channel
{
    enum State : uint8_t
    {
        OPEN,
        CLOSE,
        DESTROYED,
    };

    std::mutex guard;
    State state = OPEN;
    std::condition_variable_any change;
};

Valve::~Valve()
{
    if (channel)
    {
        {
            const std::scoped_lock lock(channel->guard);
            channel->state = Channel::DESTROYED;
        }
        channel->change.notify_all();
    }
}
Valve::Valve(Valve&& other) noexcept : channel(std::move(other.channel))
{
}
Valve& Valve::operator=(Valve&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }
    channel = std::move(other.channel);
    return *this;
}
bool Valve::apply_pressure()
{
    auto old = Channel::CLOSE;
    {
        const std::scoped_lock lock(channel->guard);
        old = channel->state;
        channel->state = Channel::CLOSE;
    }
    return old == Channel::OPEN;
}
bool Valve::release_pressure()
{
    auto old = Channel::OPEN;
    {
        const std::scoped_lock lock(channel->guard);
        old = channel->state;
        channel->state = Channel::OPEN;
    }
    if (old == Channel::CLOSE)
    {
        channel->change.notify_all();
        return true;
    }
    return false;
}
void Ingestion::wait(const std::stop_token& stopToken) const
{
    std::unique_lock lock(channel->guard);
    if (channel->state == Channel::State::OPEN)
    {
        return;
    }
    bool destroyed = false;
    channel->change.wait(
        lock,
        stopToken,
        [&]()
        {
            const auto state = channel->state;
            destroyed = state == Channel::DESTROYED;
            return destroyed || state == Channel::OPEN;
        });

    INVARIANT(!destroyed, "Valve was destroyed before the Ingestion");
}

std::pair<Valve, Ingestion> Backpressure()
{
    auto channel = std::make_shared<Channel>();
    return {Valve{channel}, Ingestion{channel}};
}
