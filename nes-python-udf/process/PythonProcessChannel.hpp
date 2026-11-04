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
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{

/// Bounded queue between the `PythonProcess` sink and source of one UDF operator, which are the two halves of a split plan and
/// run on the same worker. Worker threads only ever queue a buffer here; the source thread waits for the process.
class PythonProcessChannel
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

    enum class Role : uint8_t
    {
        SINK,
        SOURCE,
    };

    /// Each half holds one lease from its construction to its destruction, which also runs on a hard stop. They are created in
    /// either order. The source's lease removes the channel when it ends. The sink's does so only if no source ever came and the
    /// sink was not closed; a closed channel stays so that a source that comes later ends its stream.
    class Lease
    {
    public:
        Lease(std::string id, Role role);
        ~Lease();

        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&&) = delete;
        Lease& operator=(Lease&&) = delete;

        PythonProcessChannel* operator->() const { return channel.get(); }

        /// Ends the lease before the destructor does. Further calls do nothing.
        void release();

    private:
        std::string id;
        Role role;
        std::shared_ptr<PythonProcessChannel> channel;
    };

    [[nodiscard]] PushResult tryPush(const TupleBuffer& buffer);
    /// Blocks until a buffer arrives. Nullopt once the channel is closed and drained, or on a stop request.
    std::optional<TupleBuffer> pop(const std::stop_token& stopToken);
    /// No further buffers will be pushed.
    void close();

private:
    /// The process handles one batch at a time, so this only absorbs bursts.
    static constexpr size_t CAPACITY = 16;

    std::mutex mutex;
    std::condition_variable_any changed;
    std::deque<TupleBuffer> buffers;
    bool closed = false;
    bool sourceAttached = false;
    bool consumerGone = false;
};

}
