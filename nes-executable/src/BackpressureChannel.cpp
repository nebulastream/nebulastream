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
#include <stop_token>
#include <utility>

#include <folly/Synchronized.h>

#include <ErrorHandling.hpp>

/// Represents the state of the backpressure channel guarded by a mutex and communicated to the listener via the condition variable.
/// The channel is initially open.
struct Channel
{
    enum class State : uint8_t
    {
        OPEN,
        CLOSED,
        DESTROYED,
    };

    folly::Synchronized<State, std::mutex> stateMtx{State::OPEN};
    std::condition_variable_any change;
};

BackpressureController::BackpressureController(std::shared_ptr<Channel> channel) : channel{std::move(channel)}
{
}

BackpressureController::~BackpressureController()
{
    if (channel)
    {
        *channel->stateMtx.lock() = Channel::State::DESTROYED;
        channel->change.notify_all();
    }
}

bool BackpressureController::applyPressure()
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::State::CLOSED);
    INVARIANT(
        old != Channel::State::DESTROYED, "The backpressureController is still alive thus the channel should not have been destroyed");
    return old == Channel::State::OPEN;
}

bool BackpressureController::releasePressure()
{
    const auto old = std::exchange(*channel->stateMtx.lock(), Channel::State::OPEN);
    INVARIANT(
        old != Channel::State::DESTROYED, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
    if (old == Channel::State::CLOSED)
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
    /// If the channel is open, backpressureListener can proceed
    if (*state == Channel::State::OPEN)
    {
        return;
    }

    bool destroyed = false;
    /// Wait for the channel state to change
    channel->change.wait(
        state.as_lock(),
        stopToken,
        [&destroyed, &state] -> bool
        {
            destroyed = *state == Channel::State::DESTROYED;
            return destroyed || *state == Channel::State::OPEN;
        });

    INVARIANT(!destroyed, "Backpressure Controller was destroyed before the BackpressureListener");
}

std::pair<BackpressureController, BackpressureListener> createBackpressureChannel()
{
    const auto channel = std::make_shared<Channel>();
    return {BackpressureController{channel}, BackpressureListener{channel}};
}
