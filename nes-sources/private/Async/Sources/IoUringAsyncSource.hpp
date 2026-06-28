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
#include <optional>
#include <vector>

#include <liburing.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncSource.hpp>

namespace NES
{

/// IoUringAsyncSource is the shared base for the asio-integrated, deep-queue io_uring sources (file + TCP).
///
/// Why a base: both sources want the io_uring win -- keep MANY operations in flight from a SINGLE thread to
/// hide per-op latency (cold-read disk latency for files, recv/syscall latency for sockets) -- but they live
/// in the ASYNC framework, where many sources share one asio io_context thread. So we must NOT block that
/// thread waiting on the ring. This base owns the ring and bridges its completions into the asio reactor: an
/// eventfd is registered with the ring (io_uring_register_eventfd), wrapped in an asio descriptor, and the
/// derived source `co_await`s a read on it -- suspending the coroutine (yielding the thread to sibling
/// sources) until the kernel signals a completion. This is the asio-native analog of the blocking source's
/// io_uring_wait_cqe (which is fine on its own dedicated thread, but would stall every other async source here).
///
/// It uses the PREFILL model (preFillsBuffers() == true): the source acquires its own pool buffers (from the
/// pool handed to open()) and keeps the ring full, then hands completed buffers to the runner in order via
/// takePreFilledBuffer() -- the runner never pre-acquires (that would contend with the ring and deadlock).
/// Derived classes implement onOpen() (transport setup + initial submissions), onClose(), and the reaping
/// takePreFilledBuffer().
class IoUringAsyncSource : public AsyncSource
{
public:
    IoUringAsyncSource();
    ~IoUringAsyncSource() override;

    IoUringAsyncSource(const IoUringAsyncSource&) = delete;
    IoUringAsyncSource& operator=(const IoUringAsyncSource&) = delete;
    IoUringAsyncSource(IoUringAsyncSource&&) = delete;
    IoUringAsyncSource& operator=(IoUringAsyncSource&&) = delete;

    /// Store the pool + resolve the queue depth, then delegate to onOpen() (which finalizes the depth if
    /// needed, calls initRing(), and primes the ring).
    asio::awaitable<void, Executor> open(std::shared_ptr<AbstractBufferProvider> bufferProvider) final;

    /// Unused: preFillsBuffers() is true, so the runner drains takePreFilledBuffer() instead.
    asio::awaitable<InternalSourceResult, Executor> fillBuffer(TupleBuffer& buffer) final;

    [[nodiscard]] bool preFillsBuffers() const final { return true; }

    /// Tear down the eventfd bridge + ring (after the derived onClose() releases its transport).
    void close() final;

protected:
    /// Derived transport open + initial ring priming (file: open fd, submit QD reads; tcp: connect, arm recv).
    /// Must call co_await initRing() once (after finalizing queueDepth) before submitting any operation.
    virtual asio::awaitable<void, Executor> onOpen() = 0;

    /// Create the io_uring ring (sized to the current queueDepth) and the eventfd completion bridge. Called by
    /// onOpen() once the derived class has settled on its final queueDepth.
    asio::awaitable<void, Executor> initRing();
    /// Derived transport teardown (close fd / socket, free buffer ring). Called before the ring is exited.
    virtual void onClose() = 0;

    /// Suspend the coroutine until the ring signals at least one completion (via the registered eventfd).
    /// Returns false if the wait was cancelled/aborted (query stop) -- the caller should then end the stream.
    asio::awaitable<bool, Executor> waitForCompletion();

    /// Acquire one buffer from the source's pool, yielding to cancellation. nullopt => a stop was requested.
    /// NB: under backpressure (pool drained) this briefly blocks the io_context thread on the pool timeout --
    /// the same trade-off the existing async runner makes with getBufferBlocking(); documented, not yet ideal.
    asio::awaitable<std::optional<TupleBuffer>, Executor> acquireBuffer();

    /// Effective ring depth: NES_IOURING_QD overrides; otherwise derived in open() from the pool.
    /// Derived classes may further constrain it (e.g. TCP rounds it to a power of two for the buffer ring).
    [[nodiscard]] unsigned resolveQueueDepth(std::size_t poolBuffers) const;

    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    std::size_t bufferSize = 0;

    io_uring ring{};
    bool ringInitialized = false;

    int eventFd = -1;
    std::optional<asio::posix::stream_descriptor> eventDescriptor;

    unsigned queueDepth = 16;
    bool autoQueueDepth = true;

    std::vector<io_uring_cqe*> cqeBatch; ///< scratch for io_uring_peek_batch_cqe (drain many CQEs per wakeup)
};

}
