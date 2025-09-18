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
#include <utility>

struct Channel;
class Ingestion;
class Valve;

/// This is the entrypoint to a backpressure channel. It creates a pair of connected Valve and Ingestion.
/// A Valve controls the Backpressure and an Ingestion only allows further progress if there is no backpressure.
/// In NebulaStream a Valve is owned by exactly one sink, which controls all the Ingestion of all sources within the same query plan.
/// Currently, the Backpressure channel enforces the invariant that sinks always outlive sources. Thus, if a Valve is destroyed, all
/// connected Ingestions that are still alive and in use will report an assertion failure.
std::pair<Valve, Ingestion> Backpressure();

/// A Valve is the exclusive controller of a backpressure channel. It allows the user to apply and release backpressure, which blocks
/// or unblocks all connected Ingestions.
class Valve
{
    explicit Valve(std::shared_ptr<Channel> channel);

    std::shared_ptr<Channel> channel;
    friend std::pair<Valve, Ingestion> Backpressure();

public:
    ~Valve();

    /// Currently, a valve represents unique ownership over the backpressure channel, thus copying is not enabled.
    Valve(const Valve& other) = delete;
    Valve& operator=(const Valve& other) = delete;

    /// Default moves leaves channel in an empty state which prevents unintended destruction of the underlying channel
    Valve(Valve&& other) noexcept = default;
    Valve& operator=(Valve&& other) noexcept = default;

    bool applyPressure();
    bool releasePressure();
};

/// Listener of the backpressure channel is the Ingestion type that is used by sources.
/// Before initiating a read of a new buffer, the source can if backpressure has been requested by a sink with a call to `wait`.
/// This will cause the thread to block on the call if backpressure has been applied, until pressure is released by a sink, in which case
/// the thread will be notified via the condition_variable in the channel.
class Ingestion
{
    explicit Ingestion(std::shared_ptr<Channel> channel) : channel(std::move(channel)) { }

    friend std::pair<Valve, Ingestion> Backpressure();
    std::shared_ptr<Channel> channel;

public:
    void wait(const std::stop_token& stopToken) const;
};
