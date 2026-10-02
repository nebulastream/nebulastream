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

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>

#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Bounded in-process handoff of TupleBuffers between the two halves of an
/// asynchronously executed operator: a producer sink at the end of one pipeline and a
/// consumer source at the start of the next.
///
/// Buffers are handed over without copying — the queue only holds a reference, so the
/// underlying memory is returned to the pool once the consumer is done with it.
///
/// Thread-safe. One producer and one consumer is the intended use, but nothing here
/// assumes it.
class HandoffChannel
{
public:
    explicit HandoffChannel(size_t capacity);

    HandoffChannel(const HandoffChannel&) = delete;
    HandoffChannel& operator=(const HandoffChannel&) = delete;
    HandoffChannel(HandoffChannel&&) = delete;
    HandoffChannel& operator=(HandoffChannel&&) = delete;
    ~HandoffChannel() = default;

    /// Enqueues a buffer. Returns false when the channel is full — the producer's cue to
    /// stash the buffer and retry, the way `NetworkSink` reacts to a full send queue — or
    /// when the channel has already been closed.
    bool tryPush(TupleBuffer buffer);

    /// Returns the next buffer, blocking until one is available.
    ///
    /// `std::nullopt` means no further buffer will arrive, for one of two reasons: the
    /// producer closed the channel and everything queued has been handed out, or
    /// `stopToken` was triggered. Callers distinguish the two by inspecting the token;
    /// `SourceThread` does exactly that before deciding whether to report end of stream.
    std::optional<TupleBuffer> popBlocking(const std::stop_token& stopToken);

    /// Marks the producer side as finished. Buffers that are already queued are still
    /// handed out; only once the queue runs empty does `popBlocking` report the end.
    /// Drain first, then close.
    void close();

    [[nodiscard]] bool isClosed() const;
    [[nodiscard]] size_t size() const;
    [[nodiscard]] size_t capacity() const;

private:
    mutable std::mutex mutex;
    std::condition_variable_any notEmpty;
    std::deque<TupleBuffer> queue;
    size_t capacityLimit;
    bool closed = false;
};

/// Process-wide directory of channels, keyed by the id that both halves carry in their
/// descriptors. Descriptors can only hold plain data, never pointers, so this
/// indirection is how a sink and a source find the same channel — the same trick
/// `NetworkSource` uses to reach its transport.
///
/// Entries are held weakly: once both halves are gone the channel is destroyed and the
/// buffers it still holds go back to the pool. That matters because `BufferManager`
/// fails hard on leaked buffers at shutdown.
class HandoffChannelRegistry
{
public:
    /// Returns the channel for `channelId`, creating it on first use. Either half may
    /// arrive first; the deployment order of the two plans is not guaranteed.
    /// `capacity` applies only if the channel is created by this call.
    static std::shared_ptr<HandoffChannel> getOrCreate(const std::string& channelId, size_t capacity);

    /// Returns the channel if it currently exists, otherwise nullptr.
    static std::shared_ptr<HandoffChannel> find(const std::string& channelId);

    /// Forgets the entry. The channel itself lives on while either half still holds it.
    static void drop(const std::string& channelId);

    /// Number of live entries. For tests.
    static size_t registeredChannels();
};

}
