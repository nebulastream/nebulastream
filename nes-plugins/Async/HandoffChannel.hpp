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
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Bounded in-process queue between a `Handoff` sink and the `Async` source with the same channel id.
/// Buffers are passed on without a copy.
class HandoffChannel
{
public:
    enum class PushResult : uint8_t
    {
        PUSHED,
        /// The channel is full, or no source has attached yet. The caller retries later.
        FULL,
        /// The source has stopped and will never take a buffer again.
        CONSUMER_GONE,
    };

    /// Buffers a channel holds unless the plan decomposer sets `async.channel_capacity`.
    static constexpr size_t DEFAULT_CAPACITY = 64;

    enum class Role : uint8_t
    {
        SINK,
        SOURCE,
    };

    /// Each half holds one lease from its construction to its destruction, which also runs on a hard stop, when the
    /// engine skips `stop`. The halves are created in either order; the first creates the channel with its capacity.
    ///
    /// Until a source leased the channel, it takes no buffers, so a channel without a source is always empty. The lease
    /// that goes last removes the channel:
    /// - The source's lease removes it. A sink that still holds it is told that nobody takes its buffers any more, and
    ///   the buffers that were waiting are dropped.
    /// - The sink's lease removes it if no source ever leased it and the sink was not closed: the query was torn down,
    ///   and an empty channel is all there is to drop. A source that is created after all creates a channel of its own,
    ///   which nobody fills; the teardown stops it as well.
    /// - A sink that was closed without a source leaves the closed, empty channel behind, so that a source that comes
    ///   later ends its stream instead of waiting for a sink that is gone. If no source comes, the empty channel stays.
    class Lease
    {
    public:
        Lease(std::string id, size_t capacity, Role role);
        ~Lease();

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&&) = delete;
        Lease& operator=(Lease&&) = delete;

        HandoffChannel* operator->() const { return channel.get(); }
        /// Ends the lease before the destructor does. Further calls do nothing.
        void release();

    private:
        std::string id;
        Role role;
        std::shared_ptr<HandoffChannel> channel;
    };

    explicit HandoffChannel(size_t capacity);

    [[nodiscard]] PushResult tryPush(const TupleBuffer& buffer);
    /// Blocks until a buffer arrives. Nullopt once the channel is closed and drained, or on a stop request.
    std::optional<TupleBuffer> pop(const std::stop_token& stopToken);
    /// Nullopt if no buffer is waiting.
    std::optional<TupleBuffer> tryPop();
    /// Closed and drained: no buffer will arrive any more.
    [[nodiscard]] bool exhausted();
    /// No further buffers will be pushed.
    void close();

    /// Called after every push and on close, under the channel's lock, so it must neither block nor call back into the
    /// channel. Replaces the previous listener; an empty function removes it, after which it is never called again. The
    /// source's lease removes it when it ends.
    void setListener(std::function<void()> onChange);

private:
    std::mutex mutex;
    std::condition_variable_any changed;
    std::deque<TupleBuffer> buffers;
    size_t capacity;
    bool closed = false;
    bool sourceAttached = false;
    bool consumerGone = false;
    std::function<void()> listener;
};

}
