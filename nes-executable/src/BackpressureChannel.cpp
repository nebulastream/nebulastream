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
#include <memory>
#include <mutex>
#include <stop_token>
#include <utility>
#include <vector>

#include <folly/Synchronized.h>

#include <ErrorHandling.hpp>

/// Represents the state of the backpressure channel guarded by a mutex and communicated to the listener via the condition variable.
/// The channel is initially open; it counts the controllers currently applying pressure, so a channel shared by multiple
/// controllers (one per sink) only lets listeners proceed while NO controller applies pressure.
struct Channel
{
    struct State
    {
        /// Number of controllers currently applying pressure; the channel is open iff this is zero.
        size_t pressuring{0};
        /// Set when any controller is destroyed; listeners must be gone by then (sinks outlive sources).
        bool destroyed{false};
    };

    folly::Synchronized<State, std::mutex> stateMtx;
    std::condition_variable_any change;
};

BackpressureController::BackpressureController(std::shared_ptr<Channel> channel) : channel{std::move(channel)}
{
}

BackpressureController::~BackpressureController()
{
    if (channel)
    {
        channel->stateMtx.lock()->destroyed = true;
        channel->change.notify_all();
    }
}

bool BackpressureController::applyPressure()
{
    const auto state = channel->stateMtx.lock();
    INVARIANT(!state->destroyed, "The backpressureController is still alive thus the channel should not have been destroyed");
    if (applied)
    {
        return false;
    }
    applied = true;
    return ++state->pressuring == 1;
}

bool BackpressureController::releasePressure()
{
    const auto state = channel->stateMtx.lock();
    INVARIANT(!state->destroyed, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
    if (!applied)
    {
        return false;
    }
    applied = false;
    if (--state->pressuring == 0)
    {
        /// The last pressuring controller released, wake up all waiting BackpressureListeners
        channel->change.notify_all();
        return true;
    }
    return false;
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    auto state = channel->stateMtx.lock();
    /// If no controller applies pressure, backpressureListener can proceed
    if (!state->destroyed && state->pressuring == 0)
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
            destroyed = state->destroyed;
            return destroyed || state->pressuring == 0;
        });

    INVARIANT(!destroyed, "Backpressure Controller was destroyed before the BackpressureListener");
}

std::pair<BackpressureController, BackpressureListener> createBackpressureChannel()
{
    const auto channel = std::make_shared<Channel>();
    return {BackpressureController{channel}, BackpressureListener{channel}};
}

std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(const size_t controllerCount)
{
    PRECONDITION(controllerCount > 0, "A backpressure channel needs at least one controller");
    const auto channel = std::make_shared<Channel>();
    std::vector<BackpressureController> controllers;
    controllers.reserve(controllerCount);
    for (size_t i = 0; i < controllerCount; ++i)
    {
        controllers.emplace_back(BackpressureController{channel});
    }
    return {std::move(controllers), BackpressureListener{channel}};
}
