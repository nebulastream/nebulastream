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
/// In NebulaStream a Backpressure Controller is owned by a sink, which controls all the BackpressureListener of all sources within the same query plan.
/// Sinks must outlive sources: once the last Backpressure Controller is destroyed, every listener still in use reports an assertion failure.
std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();

/// Creates one channel with a controller per sink of a query. It applies backpressure while at least one of them does.
std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(size_t numberOfControllers);

/// A Backpressure Controller controls a backpressure channel. It allows the user to apply and release backpressure, which blocks
/// or unblocks all connected Ingestions. With more than one controller, Ingestions are blocked while any one of them applies it.
class BackpressureController
{
    explicit BackpressureController(std::shared_ptr<Channel> channel);

    std::shared_ptr<Channel> channel;
    /// Applying twice from the same controller is a no-op, as it was when a channel had a single controller.
    bool applyingPressure = false;
    friend std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();
    friend std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(size_t numberOfControllers);

public:
    ~BackpressureController();

    /// A Backpressure Controller owns its own share of the channel, thus copying is not enabled.
    BackpressureController(const BackpressureController& other) = delete;
    BackpressureController& operator=(const BackpressureController& other) = delete;

    /// Default moves leaves channel in an empty state which prevents unintended destruction of the underlying channel
    BackpressureController(BackpressureController&& other) noexcept = default;
    BackpressureController& operator=(BackpressureController&& other) noexcept = default;

    bool applyPressure();
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
    friend std::pair<std::vector<BackpressureController>, BackpressureListener> createBackpressureChannel(size_t numberOfControllers);
    std::shared_ptr<Channel> channel;

public:
    void wait(const std::stop_token& stopToken) const;
};
