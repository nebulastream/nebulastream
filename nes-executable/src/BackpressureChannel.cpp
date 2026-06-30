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

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <utility>

#include <folly/Synchronized.h>

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

    folly::Synchronized<State, std::mutex> stateMtx{OPEN};
    std::condition_variable_any change;
};

BackpressureController::BackpressureController(std::shared_ptr<Channel> channel) : channel{std::move(channel)}
{
}

BackpressureController::~BackpressureController()
{
    if (channel)
    {
        *channel->stateMtx.lock() = Channel::DESTROYED;
        channel->change.notify_all();
    }
}

bool BackpressureController::applyPressure()
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::CLOSED);
    INVARIANT(old != Channel::DESTROYED, "The backpressureController is still alive thus the channel should not have been destroyed");
    return old == Channel::OPEN;
}

bool BackpressureController::releasePressure()
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::OPEN);
    INVARIANT(old != Channel::DESTROYED, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
    if (old == Channel::CLOSED)
    {
        /// The Backpressure Controller was opened, wake up all waiting BackpressureListeners
        channel->change.notify_all();
        return true;
    }
    return false;
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    auto state = channel->stateMtx.lock();

    bool destroyed = false;
    const auto channelChanged = [&destroyed, &state] -> bool
    {
        destroyed = *state == Channel::DESTROYED;
        return destroyed || *state == Channel::OPEN;
    };

    /// Bounded poll instead of an unbounded wait: a missed condition_variable_any/stop_token wakeup (a known
    /// hazard on some libc++/libstdc++ versions) must not park a source forever and stall query teardown. We
    /// re-arm on a short timeout and re-check the stop token, but still only return once the channel is OPEN/
    /// DESTROYED or a stop was requested -- so backpressure semantics are preserved (the source does not resume
    /// producing while the channel is CLOSED).
    while (!channelChanged() && !stopToken.stop_requested())
    {
        channel->change.wait_for(state.as_lock(), stopToken, std::chrono::milliseconds{100}, channelChanged);
    }

    INVARIANT(!destroyed, "Backpressure Controller was destroyed before the BackpressureListener");
}

std::pair<BackpressureController, BackpressureListener> createBackpressureChannel()
{
    const auto channel = std::make_shared<Channel>();
    return {BackpressureController{channel}, BackpressureListener{channel}};
}
