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

/// Controller of the backpressure channel is the Valve type that is used by sinks.
/// A sink can:
/// a) apply backpressure by setting the channel state to CLOSED, which will cause the listener of the backpressure channel
/// (a source using the Ingestion type) to block on the `wait` call until the channel state changes.
/// b) release backpressure by setting the channel state to OPEN and notifying the listener of this change, unblocking threads in the `wait` call.
class Valve
{
public:
    ~Valve();

    Valve(const Valve& other) = delete;
    Valve(Valve&& other) noexcept;
    Valve& operator=(const Valve& other) = delete;
    Valve& operator=(Valve&& other) noexcept;

    bool applyPressure();
    bool releasePressure();

    friend std::pair<Valve, Ingestion> Backpressure();

private:
    explicit Valve(std::shared_ptr<Channel> channel) : channel(std::move(channel)) { }

    std::shared_ptr<Channel> channel;
};

/// Listener of the backpressure channel is the Ingestion type that is used by sources.
/// Before initiating a read of a new buffer, the source can if backpressure has been requested by a sink with a call to `wait`.
/// This will cause the thread to block on the call if backpressure has been applied, until pressure is released by a sink, in which case
/// the thread will be notified via the condition_variable in the channel.
class Ingestion
{
public:
    void wait(const std::stop_token& stopToken) const;

    friend std::pair<Valve, Ingestion> Backpressure();

private:
    explicit Ingestion(std::shared_ptr<Channel> channel) : channel(std::move(channel)) { }

    std::shared_ptr<Channel> channel;
};

std::pair<Valve, Ingestion> Backpressure();
