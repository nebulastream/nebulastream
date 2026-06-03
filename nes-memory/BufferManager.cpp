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
#include <Runtime/BufferManager.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <utility>
#include <ittnotify.h>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

BufferManager::BufferManager(
    Private, const uint32_t bufferSize, const uint32_t numOfBuffers, std::shared_ptr<std::pmr::memory_resource> memoryResource)
    : availableBuffers(numOfBuffers)
    , unpooledChunksManager(std::make_shared<UnpooledChunksManager>(memoryResource))
    , bufferSize(bufferSize)
    , numOfBuffers(numOfBuffers)
    , memoryResource(std::move(memoryResource))
{
    initialize();
}

std::shared_ptr<BufferManager>
BufferManager::create(uint32_t bufferSize, uint32_t numOfBuffers, const std::shared_ptr<std::pmr::memory_resource>& memoryResource)
{
    return std::make_shared<BufferManager>(Private{}, bufferSize, numOfBuffers, memoryResource);
}

BufferManager::~BufferManager()
{
    destroy();
}

void BufferManager::destroy()
{
    bool expected = false;
    NES_DEBUG("Calling BufferManager::destroy()");
    if (isDestroyed.compare_exchange_strong(expected, true))
    {
        bool success = true;
        if (allBuffers.size() != getNumberOfAvailableBuffers())
        {
            NES_ERROR("[BufferManager] total buffers {} :: available buffers {}", allBuffers.size(), getNumberOfAvailableBuffers());
            success = false;
        }
        for (auto& buffer : allBuffers)
        {
            if (!buffer.isAvailable())
            {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
                buffer.controlBlock->dumpOwningThreadInfo();
#endif
                NES_ERROR("[BufferManager] leaked buffer detected: segment at {}", fmt::ptr(buffer.ptr));
                success = false;
            }
        }
        INVARIANT(
            success,
            "Requested buffer manager shutdown but a buffer is still used allBuffers={} available={}",
            allBuffers.size(),
            getNumberOfAvailableBuffers());
        /// RAII takes care of deallocating memory here
        allBuffers.clear();

        availableBuffers = decltype(availableBuffers)();
        NES_DEBUG("Shutting down Buffer Manager completed");
        memoryResource->deallocate(basePointer, allocatedAreaSize, DEFAULT_ALIGNMENT);
        allocatedAreaSize = 0;

        /// Destroying the unpooled chunks
        unpooledChunksManager.reset();
    }
}

void BufferManager::initialize()
{
    /// Buffers are always aligned to a cache line; the alignment is no longer configurable.
    constexpr uint32_t withAlignment = DEFAULT_ALIGNMENT;
    const size_t pages = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    auto memorySizeInBytes = pages * page_size;

    uint64_t requiredMemorySpace = this->bufferSize * this->numOfBuffers;
    double percentage = (100.0 * requiredMemorySpace) / memorySizeInBytes;
    NES_DEBUG("NES memory allocation requires {} out of {} (so {}%) available bytes", requiredMemorySpace, memorySizeInBytes, percentage);

    INVARIANT(
        requiredMemorySpace < memorySizeInBytes,
        "NES tries to allocate more memory than physically available requested={} available={}",
        requiredMemorySpace,
        memorySizeInBytes);
    if (withAlignment > 0)
    {
        PRECONDITION(
            !(withAlignment & (withAlignment - 1)), "NES tries to align memory but alignment={} is not a pow of two", withAlignment);
    }
    PRECONDITION(withAlignment <= page_size, "NES tries to align memory but alignment is invalid: {} <= {}", withAlignment, page_size);

    PRECONDITION(
        alignof(detail::BufferControlBlock) <= withAlignment,
        "Requested alignment is too small, must be at least {}",
        alignof(detail::BufferControlBlock));

    allBuffers.reserve(numOfBuffers);
    auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), withAlignment);
    auto alignedBufferSize = alignBufferSize(bufferSize, withAlignment);
    allocatedAreaSize = alignBufferSize(controlBlockSize + alignedBufferSize, withAlignment);
    const size_t offsetBetweenBuffers = allocatedAreaSize;
    allocatedAreaSize *= numOfBuffers;
    basePointer = static_cast<uint8_t*>(memoryResource->allocate(allocatedAreaSize, withAlignment));

#ifndef NDEBUG
    constexpr std::array marker{'N', 'E', 'B', 'U', 'S', 'T', 'R', 'M'};
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): basePointer is freshly allocated raw storage aligned to >= alignof(uint64_t).
    std::fill_n(reinterpret_cast<uint64_t*>(basePointer), allocatedAreaSize / sizeof(uint64_t), std::bit_cast<uint64_t>(marker));
#endif

    NES_TRACE(
        "Allocated {} bytes with alignment {} buffer size {} num buffer {} controlBlockSize {} {}",
        allocatedAreaSize,
        withAlignment,
        alignedBufferSize,
        numOfBuffers,
        controlBlockSize,
        alignof(detail::BufferControlBlock));

    INVARIANT(basePointer, "memory allocation failed, because 'basePointer' was a nullptr");
    uint8_t* ptr = basePointer;
    for (size_t i = 0; i < numOfBuffers; ++i)
    {
        uint8_t* controlBlock = ptr;
        uint8_t* payload = ptr + controlBlockSize;
        allBuffers.emplace_back(
            payload,
            bufferSize,
            [](detail::MemorySegment* segment, BufferRecycler* recycler) { recycler->recyclePooledBuffer(segment); },
            controlBlock);

        availableBuffers.write(&allBuffers.back());
        ptr += offsetBetweenBuffers;
    }
    NES_DEBUG("BufferManager configuration bufferSize={} numOfBuffers={}", this->bufferSize, this->numOfBuffers);
}

TupleBuffer BufferManager::getBufferBlocking()
{
    auto buffer = getBufferWithTimeout(GET_BUFFER_TIMEOUT);
    if (buffer.has_value())
    {
        return buffer.value();
    }
    /// Throw exception if no buffer was returned allocated after timeout.
    throw BufferAllocationFailure("Global buffer pool could not allocate buffer before timeout({})", GET_BUFFER_TIMEOUT);
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking()
{
    detail::MemorySegment* memSegment = nullptr;
    if (!availableBuffers.read(memSegment))
    {
        return std::nullopt;
    }
    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

namespace
{
__itt_domain* domain = __itt_domain_create("buffermanager");
__itt_string_handle* waitForBuffer = __itt_string_handle_create("Wait for Buffer");
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    detail::MemorySegment* memSegment = nullptr;

    /// fast path
    if (!availableBuffers.read(memSegment))
    {
        __itt_task_begin(domain, __itt_null, __itt_null, waitForBuffer);
        const auto deadline = std::chrono::steady_clock::now() + timeoutMs;
        if (!availableBuffers.tryReadUntil(deadline, memSegment))
        {
            __itt_task_end(domain);
            return std::nullopt;
        }
        __itt_task_end(domain);
    }

    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(const size_t bufferSize)
{
    return unpooledChunksManager->getUnpooledBuffer(bufferSize, DEFAULT_ALIGNMENT, shared_from_this());
}

void BufferManager::recyclePooledBuffer(detail::MemorySegment* segment)
{
    INVARIANT(segment->isAvailable(), "Recycling buffer callback invoked on used memory segment");
    INVARIANT(
        segment->controlBlock->owningBufferRecycler == nullptr, "Buffer should not retain a reference to its parent while not in use");
    std::coroutine_handle<> waiterToResume;
    {
        std::lock_guard lock(bufferWaitersMutex);
        if (!bufferWaiters.empty())
        {
            /// Hand the freed segment directly to a parked coroutine rather than routing it through
            /// the pool: prepare it exactly as getBufferNoBlocking() would, store the buffer in the
            /// awaiter, and resume. The resumed coroutine is guaranteed a buffer and never retries.
            auto* waiter = bufferWaiters.front();
            bufferWaiters.pop_front();
            USED_IN_DEBUG const auto prepared = segment->controlBlock->prepare(shared_from_this());
            INVARIANT(prepared, "[BufferManager] handed-off buffer had an invalid reference counter");
            /// Construct here (rather than optional::emplace) so the friend-only TupleBuffer
            /// constructor is reached from BufferManager, then move into the awaiter's optional.
            waiter->buffer = TupleBuffer(segment->controlBlock.get(), segment->ptr, segment->size);
            waiterToResume = waiter->handle;
        }
        else
        {
            USED_IN_DEBUG const auto couldRecycleBuffer = availableBuffers.writeIfNotFull(segment);
            INVARIANT(couldRecycleBuffer, "should always succeed");
        }
    }
    /// Resume outside the lock: the releasing thread drives the resumed coroutine until it next
    /// suspends (or returns), mirroring AsyncSemaphore::release().
    if (waiterToResume)
    {
        waiterToResume.resume();
    }
}

void BufferManager::recycleUnpooledBuffer(detail::MemorySegment*, const AllocationThreadInfo&)
{
    INVARIANT(false, "This method should not be called!");
}

bool BufferManager::BufferAvailableAwaiter::await_suspend(std::coroutine_handle<> awaiting) noexcept
{
    std::lock_guard lock(manager.bufferWaitersMutex);
    /// Re-check under the lock: a buffer may have been returned to the pool between the fast-path
    /// getBufferNoBlocking() in getBufferAsync() and acquiring the lock. Taking it here instead of
    /// parking closes the lost-wakeup window against recyclePooledBuffer().
    if (auto available = manager.getBufferNoBlocking())
    {
        buffer.emplace(std::move(*available));
        return false;
    }
    handle = awaiting;
    manager.bufferWaiters.push_back(this);
    return true;
}

TupleBuffer BufferManager::BufferAvailableAwaiter::await_resume() noexcept
{
    return std::move(*buffer);
}

coro::task<TupleBuffer> BufferManager::getBufferAsync()
{
    /// Fast path: take a buffer straight from the pool without parking.
    if (auto buffer = getBufferNoBlocking())
    {
        co_return std::move(*buffer);
    }
    /// Slow path: park until recyclePooledBuffer() hands us a buffer directly.
    co_return co_await BufferAvailableAwaiter{*this};
}

size_t BufferManager::getBufferSize() const
{
    return bufferSize;
}

size_t BufferManager::getNumOfPooledBuffers() const
{
    return numOfBuffers;
}

size_t BufferManager::getNumOfUnpooledBuffers() const
{
    return unpooledChunksManager->getNumberOfUnpooledBuffers();
}

size_t BufferManager::getNumberOfAvailableBuffers() const
{
    /// If there are pending reads the queue may report negative values. This effectivly means its empty.
    return static_cast<size_t>(std::max(availableBuffers.size(), static_cast<ssize_t>(0)));
}

BufferManagerType BufferManager::getBufferManagerType() const
{
    return BufferManagerType::GLOBAL;
}

}
