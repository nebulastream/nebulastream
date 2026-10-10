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

#include <memory>
#include <stop_token>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>

struct Channel;
class BackpressureListener;
class BackpressureController;

/// This is the entrypoint to backpressure. It creates one channel per origin, a single Backpressure Controller controlling all of them,
/// and one BackpressureListener per origin. A Backpressure Controller controls the Backpressure, and a BackpressureListener only allows
/// further progress if there is no backpressure on its channel.
/// In NebulaStream every source of a query plan owns the listener of its origin. Copies of the Backpressure Controller can be handed to any
/// component that needs to throttle sources, e.g., the sink.
/// Currently, the Backpressure channel enforces the invariant that controllers always outlive sources. Thus, if the last copy of a
/// Backpressure Controller is destroyed, all connected BackpressureListeners that are still alive and in use will report an assertion failure.
std::pair<BackpressureController, std::unordered_map<NES::OriginId, BackpressureListener>>
createBackpressureChannels(const std::vector<NES::OriginId>& origins);

/// Convenience for a single channel, keyed by INITIAL<OriginId>.
std::pair<BackpressureController, BackpressureListener> createBackpressureChannel();

/// A Backpressure Controller allows the user to apply and release backpressure, which blocks or unblocks either all connected listeners or
/// only the listener of a specific origin.
/// Every copy of a Backpressure Controller is an independent holder of backpressure: a channel is blocked as long as at least one holder
/// applies pressure to it. A copy starts without holding any pressure, and destroying a holder releases all pressure it still holds.
/// A single holder must not be used concurrently.
class BackpressureController
{
    struct Channels;
    explicit BackpressureController(std::shared_ptr<Channels> channels);

    std::shared_ptr<Channels> channels;
    std::unordered_set<NES::OriginId> held;
    friend std::pair<BackpressureController, std::unordered_map<NES::OriginId, BackpressureListener>>
    createBackpressureChannels(const std::vector<NES::OriginId>& origins);

public:
    BackpressureController(const BackpressureController& other);
    BackpressureController& operator=(const BackpressureController& other);
    BackpressureController(BackpressureController&& other) noexcept;
    BackpressureController& operator=(BackpressureController&& other) noexcept;
    ~BackpressureController();

    /// Return true if this holder changed its pressure on at least one channel.
    bool applyPressure();
    bool releasePressure();

    /// Return true if this holder changed its pressure on the origin's channel.
    bool applyPressure(NES::OriginId origin);
    bool releasePressure(NES::OriginId origin);

    [[nodiscard]] bool controls(NES::OriginId origin) const;
};

/// Listener of the backpressure channel is the Ingestion type that is used by sources.
/// Before initiating a read of a new buffer, the source can if backpressure has been requested with a call to `wait`.
/// This will cause the thread to block on the call if backpressure has been applied, until pressure is released, in which case
/// the thread will be notified via the condition_variable in the channel.
/// A listener belongs to exactly one source, thus copying is not enabled.
class BackpressureListener
{
    explicit BackpressureListener(std::shared_ptr<Channel> channel) : channel(std::move(channel)) { }

    friend std::pair<BackpressureController, std::unordered_map<NES::OriginId, BackpressureListener>>
    createBackpressureChannels(const std::vector<NES::OriginId>& origins);
    std::shared_ptr<Channel> channel;

public:
    BackpressureListener(const BackpressureListener& other) = delete;
    BackpressureListener& operator=(const BackpressureListener& other) = delete;
    BackpressureListener(BackpressureListener&& other) noexcept = default;
    BackpressureListener& operator=(BackpressureListener&& other) noexcept = default;
    ~BackpressureListener() = default;

    void wait(const std::stop_token& stopToken) const;
};
