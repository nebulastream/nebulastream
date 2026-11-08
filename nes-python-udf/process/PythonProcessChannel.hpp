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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <Runtime/TupleBuffer.hpp>
#include <folly/MPMCQueue.h>

namespace NES
{

/// Bounded queue between the `PythonProcess` sink and source of one UDF operator, which are the two halves of a split plan and
/// run on the same worker. Worker threads only ever queue a buffer here; the source thread polls it next to the sockets of its processes.
/// Neither side takes a lock: the queue is a folly::MPMCQueue, which the source creates once it knows how many buffers it takes.
class PythonProcessChannel
{
public:
    enum class PushResult : uint8_t
    {
        PUSHED,
        /// The channel is full, or no source has opened yet. The caller retries later.
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

    PythonProcessChannel();
    ~PythonProcessChannel();
    PythonProcessChannel(const PythonProcessChannel&) = delete;
    PythonProcessChannel& operator=(const PythonProcessChannel&) = delete;
    PythonProcessChannel(PythonProcessChannel&&) = delete;
    PythonProcessChannel& operator=(PythonProcessChannel&&) = delete;

    [[nodiscard]] PushResult tryPush(const TupleBuffer& buffer);

    struct Popped
    {
        std::optional<TupleBuffer> buffer;
        /// Closed and drained: no buffer will come any more.
        bool ended = false;
    };

    /// Never blocks. If the channel is empty, the next push or `close` makes `notifier` readable.
    Popped tryPop();
    /// An eventfd for poll, readable after `tryPop` found the channel empty and a buffer or the end arrived since. Reading it resets it.
    [[nodiscard]] int notifier() const { return eventFd; }

    /// No further buffers will be pushed.
    void close();

    /// Creates the queue, which holds `buffers` before the sink has to retry; until then, the channel is full. The source calls it
    /// once, when it opens, with what its processes take in the retry interval of the sink, so that they do not run dry while it waits.
    void setCapacity(size_t buffers);

private:
    void notify() const;
    /// Drops the buffers in the queue.
    void drain();

    int eventFd = -1;
    std::unique_ptr<folly::MPMCQueue<TupleBuffer>> queue;
    /// `queue` once it exists.
    std::atomic<folly::MPMCQueue<TupleBuffer>*> openQueue{nullptr};
    /// The source found the channel empty and waits for `notifier`; only then does a push write to it.
    std::atomic<bool> sourceWaiting{false};
    std::atomic<bool> closed{false};
    /// Changed under the lock of the registry, see Lease.
    bool sourceAttached = false;
    std::atomic<bool> consumerGone{false};
};

}
