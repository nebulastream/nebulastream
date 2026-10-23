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
        /// The caller retries later.
        FULL,
        /// The source has stopped and will never take a buffer again.
        CONSUMER_GONE,
    };

    /// Buffers a channel holds unless the plan decomposer sets `async.channel_capacity`.
    static constexpr size_t DEFAULT_CAPACITY = 64;

    explicit HandoffChannel(size_t capacity);

    /// Both halves look the channel up by id, in whichever order they start; the first one creates it with its
    /// capacity. It stays registered until `release` so that buffers the sink handed off before the source opened
    /// are not lost.
    static std::shared_ptr<HandoffChannel> get(const std::string& id, size_t capacity);
    /// Called by the source, which is the last to use the channel. A sink that still holds the channel is told that
    /// nobody takes its buffers any more, and the buffers that were waiting are dropped.
    static void release(const std::string& id);

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
    /// channel. Replaces the previous listener; an empty function removes it, after which it is never called again.
    void setListener(std::function<void()> onChange);

private:
    std::mutex mutex;
    std::condition_variable_any changed;
    std::deque<TupleBuffer> buffers;
    size_t capacity;
    bool closed = false;
    bool consumerGone = false;
    std::function<void()> listener;
};

}
