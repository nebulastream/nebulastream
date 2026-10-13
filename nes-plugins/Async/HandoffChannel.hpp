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

/// Bounded in-process queue between a `Handoff` sink and the `Async` source with the same channel id.
/// Buffers are passed on without a copy.
class HandoffChannel
{
public:
    explicit HandoffChannel(size_t capacity);

    /// Both halves look the channel up by id, in whichever order they start. It stays registered until
    /// `release` so that buffers the sink handed off before the source opened are not lost.
    static std::shared_ptr<HandoffChannel> get(const std::string& id);
    /// Called by the source, which is the last to use the channel.
    static void release(const std::string& id);

    /// False if the channel is full; the caller retries later.
    [[nodiscard]] bool tryPush(const TupleBuffer& buffer);
    /// Blocks until a buffer arrives. Nullopt once the channel is closed and drained, or on a stop request.
    std::optional<TupleBuffer> pop(const std::stop_token& stopToken);
    /// No further buffers will be pushed.
    void close();

private:
    std::mutex mutex;
    std::condition_variable_any changed;
    std::deque<TupleBuffer> buffers;
    size_t capacity;
    bool closed = false;
};

}
