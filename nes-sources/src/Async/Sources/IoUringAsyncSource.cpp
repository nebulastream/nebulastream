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

#include <Async/Sources/IoUringAsyncSource.hpp>

#include <unistd.h>
#include <sys/eventfd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <utility>

#include <liburing.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/system/error_code.hpp>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/AsyncSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

IoUringAsyncSource::IoUringAsyncSource()
{
    if (const char* const env = std::getenv("NES_IOURING_QD"))
    {
        queueDepth = static_cast<unsigned>(std::max<unsigned long>(1, std::strtoul(env, nullptr, 10)));
        autoQueueDepth = false;
    }
}

IoUringAsyncSource::~IoUringAsyncSource()
{
    /// close() is also driven by the runner's RAII wrapper; guard against a double teardown.
    if (ringInitialized || eventFd != -1)
    {
        close();
    }
}

unsigned IoUringAsyncSource::resolveQueueDepth(const std::size_t poolBuffers) const
{
    if (not autoQueueDepth)
    {
        return queueDepth; /// NES_IOURING_QD pinned it
    }
    /// Half the pool for the in-flight ring, half as in-processing headroom, so the source never holds more
    /// than the pool allows and can always refill. Cap at 64 (QD>=32 already saturates an NVMe).
    return static_cast<unsigned>(std::clamp<std::size_t>(poolBuffers / 2, 1, 64));
}

asio::awaitable<void, Executor> IoUringAsyncSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    bufferSize = bufferProvider->getBufferSize();
    queueDepth = resolveQueueDepth(bufferProvider->getNumOfPooledBuffers());
    co_await onOpen();
}

asio::awaitable<void, Executor> IoUringAsyncSource::initRing()
{
    PRECONDITION(not ringInitialized, "initRing() must be called exactly once");
    if (const int rc = io_uring_queue_init(queueDepth, &ring, 0); rc < 0)
    {
        throw CannotOpenSource("IoUringAsyncSource: io_uring_queue_init(QD={}) failed: {}", queueDepth, std::strerror(-rc));
    }
    ringInitialized = true;
    /// 2x headroom so one peek drains a full multishot burst (TCP can post >QD completions before we reap).
    cqeBatch.assign(static_cast<std::size_t>(queueDepth) * 2, nullptr);

    eventFd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (eventFd == -1)
    {
        throw CannotOpenSource("IoUringAsyncSource: eventfd() failed: {}", std::strerror(errno));
    }
    if (const int rc = io_uring_register_eventfd(&ring, eventFd); rc < 0)
    {
        throw CannotOpenSource("IoUringAsyncSource: io_uring_register_eventfd failed: {}", std::strerror(-rc));
    }
    eventDescriptor.emplace(co_await asio::this_coro::executor, eventFd);
}

asio::awaitable<bool, Executor> IoUringAsyncSource::waitForCompletion()
{
    /// The registered eventfd is incremented by the kernel per posted completion. Reading it (8 bytes) both
    /// resets the counter and -- crucially -- suspends THIS coroutine on the asio reactor instead of blocking
    /// the shared io_context thread, so sibling sources keep running while we wait for the disk/socket.
    std::uint64_t counter = 0;
    auto [errorCode, bytesRead]
        = co_await asio::async_read(*eventDescriptor, asio::buffer(&counter, sizeof(counter)), asio::as_tuple(asio::deferred));
    (void)bytesRead;
    /// operation_aborted on query stop (the descriptor is cancelled/closed) -- tell the caller to end.
    co_return not errorCode;
}

asio::awaitable<std::optional<TupleBuffer>, Executor> IoUringAsyncSource::acquireBuffer()
{
    std::optional<TupleBuffer> buffer;
    while (not buffer.has_value())
    {
        if ((co_await asio::this_coro::cancellation_state).cancelled() == asio::cancellation_type::terminal)
        {
            co_return std::nullopt;
        }
        buffer = bufferProvider->getBufferWithTimeout(std::chrono::milliseconds(25));
    }
    co_return buffer;
}

asio::awaitable<AsyncSource::InternalSourceResult, Executor> IoUringAsyncSource::fillBuffer(TupleBuffer&)
{
    /// Unreachable: preFillsBuffers() == true, so the runner uses takePreFilledBuffer() exclusively.
    co_return EndOfStream{.bytesRead = 0};
}

void IoUringAsyncSource::close()
{
    onClose();
    if (eventDescriptor.has_value())
    {
        boost::system::error_code ignored;
        eventDescriptor->close(ignored);
        eventDescriptor.reset();
    }
    if (ringInitialized)
    {
        io_uring_queue_exit(&ring);
        ringInitialized = false;
    }
    if (eventFd != -1)
    {
        ::close(eventFd);
        eventFd = -1;
    }
}

}
