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
/// The channel is initially open and closed while at least one controller applies backpressure, so no sink releases what another still requires.
struct Channel
{
    struct State
    {
        /// The channel is closed while this is not zero.
        size_t controllersApplyingPressure = 0;
        /// Live controllers; the channel is destroyed with the last one.
        size_t controllers = 0;
        bool destroyed = false;
    };

    folly::Synchronized<State, std::mutex> stateMtx{State{}};
    std::condition_variable_any change;
};

BackpressureController::BackpressureController(std::shared_ptr<Channel> channel) : channel{std::move(channel)}
{
    ++this->channel->stateMtx.lock()->controllers;
}

BackpressureController::~BackpressureController()
{
    if (!channel)
    {
        return;
    }

    {
        auto state = channel->stateMtx.lock();
        if (applyingPressure)
        {
            --state->controllersApplyingPressure;
        }
        --state->controllers;
        state->destroyed = state->controllers == 0;
    }
    channel->change.notify_all();
}

bool BackpressureController::applyPressure()
{
    auto state = channel->stateMtx.lock();
    INVARIANT(!state->destroyed, "The backpressureController is still alive thus the channel should not have been destroyed");
    if (applyingPressure)
    {
        return false;
    }
    applyingPressure = true;
    ++state->controllersApplyingPressure;
    /// Only the controller that closes the channel reports a state change.
    return state->controllersApplyingPressure == 1;
}

bool BackpressureController::releasePressure()
{
    bool opened = false;
    {
        auto state = channel->stateMtx.lock();
        INVARIANT(!state->destroyed, "The Backpressure Controller is still alive thus the channel should not have been destroyed");
        if (!applyingPressure)
        {
            return false;
        }
        applyingPressure = false;
        --state->controllersApplyingPressure;
        opened = state->controllersApplyingPressure == 0;
    }

    if (opened)
    {
        /// The last controller released its pressure, wake up all waiting BackpressureListeners
        channel->change.notify_all();
    }
    return opened;
}

void BackpressureListener::wait(const std::stop_token& stopToken) const
{
    auto state = channel->stateMtx.lock();
    /// If no controller applies backpressure, the backpressureListener can proceed
    if (state->controllersApplyingPressure == 0)
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
            return destroyed || state->controllersApplyingPressure == 0;
        });

    INVARIANT(!destroyed, "Backpressure Controller was destroyed before the BackpressureListener");
}

std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(const size_t numberOfControllers)
{
    PRECONDITION(numberOfControllers > 0, "A backpressure channel needs at least one controller");
    const auto channel = std::make_shared<Channel>();
    std::vector<BackpressureController> controllers;
    controllers.reserve(numberOfControllers);
    for (size_t controller = 0; controller < numberOfControllers; ++controller)
    {
        controllers.emplace_back(BackpressureController{channel});
    }
    return {std::move(controllers), BackpressureListener{channel}};
}

std::pair<BackpressureController, BackpressureListener> createBackpressureChannel()
{
    auto [controllers, listener] = createBackpressureChannel(1);
    return {std::move(controllers.front()), std::move(listener)};
}
