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

#include <cstddef>
#include <memory>
#include <stop_token>
#include <utility>
#include <vector>

struct Channel;
class BackpressureListener;
class BackpressureController;

/// This is the entrypoint to a backpressure channel. It creates a pair of connected Backpressure Controller and BackpressureListener.
/// A Backpressure Controller controls the Backpressure, and a BackpressureListener only allows further progress if there is no backpressure.
/// In NebulaStream a Backpressure Controller is owned by exactly one sink, which controls all the BackpressureListener of all sources within the same query plan.
/// Currently, the Backpressure channel enforces the invariant that sinks always outlive sources. Thus, if a Backpressure Controller is destroyed, all
/// connected BackpressureListeners that are still alive and in use will report an assertion failure.
std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();

/// Multi-controller variant for query plans with multiple sinks: every sink owns one of the returned
/// controllers, and the listener blocks while ANY controller applies pressure (it unblocks only once
/// every pressuring controller has released again). With controllerCount == 1 this behaves exactly like
/// the single-controller channel. The sinks-outlive-sources invariant extends to ALL controllers: the
/// destruction of any controller invalidates the channel for still-waiting listeners.
std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(size_t controllerCount);

/// A Backpressure Controller is one of the controlling ends of a backpressure channel. It allows the user to apply and release backpressure;
/// connected Ingestions are blocked while at least one controller of the channel applies pressure.
class BackpressureController
{
    explicit BackpressureController(std::shared_ptr<Channel> channel);

    std::shared_ptr<Channel> channel;
    /// Whether THIS controller currently applies pressure; the channel counts the pressuring controllers.
    bool applied = false;
    friend std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();
    friend std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(size_t controllerCount);

public:
    ~BackpressureController();

    /// A Backpressure Controller represents unique ownership over its controlling end of the channel, thus copying is not enabled.
    BackpressureController(const BackpressureController& other) = delete;
    BackpressureController& operator=(const BackpressureController& other) = delete;

    /// Default moves leaves channel in an empty state which prevents unintended destruction of the underlying channel
    BackpressureController(BackpressureController&& other) noexcept = default;
    BackpressureController& operator=(BackpressureController&& other) noexcept = default;

    /// Returns true iff this call transitioned the CHANNEL into the pressured state (i.e. no other
    /// controller was applying pressure before). Applying pressure twice on the same controller is a no-op.
    bool applyPressure();
    /// Returns true iff this call transitioned the CHANNEL out of the pressured state (i.e. this was the
    /// last controller applying pressure). Releasing without applied pressure is a no-op.
    bool releasePressure();
};

/// Listener of the backpressure channel is the Ingestion type that is used by sources.
/// Before initiating a read of a new buffer, the source can if backpressure has been requested by a sink with a call to `wait`.
/// This will cause the thread to block on the call if backpressure has been applied, until pressure is released by a sink, in which case
/// the thread will be notified via the condition_variable in the channel.
class BackpressureListener
{
    explicit BackpressureListener(std::shared_ptr<Channel> channel) : channel(std::move(channel)) { }

    friend std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();
    friend std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(size_t controllerCount);
    std::shared_ptr<Channel> channel;

public:
    void wait(const std::stop_token& stopToken) const;
};
