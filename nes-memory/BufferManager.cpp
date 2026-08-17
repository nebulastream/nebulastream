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
#include <limits>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <utility>
#include <ittnotify.h>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/MallocUnpooledBufferManager.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/UnpooledChunksManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>
#include "Runtime/UnpooledBufferManager.hpp"
#include "Util/Sanitizer.hpp"

namespace NES
{

BufferManager::BufferManager(
    Private,
    const uint32_t bufferSize,
    const uint32_t numOfBuffers,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    const size_t unpooledMemoryLimitInBytes,
    const uint32_t alignment,
    const UnpooledBufferManagerType unpooledBufferManagerType)
    : availableBuffers(numOfBuffers)
    , unpooledBufferManager(
          unpooledBufferManagerType == UnpooledBufferManagerType::MALLOC
              ? std::shared_ptr<UnpooledBufferManager>{std::make_shared<MallocUnpooledBufferManager>(unpooledMemoryLimitInBytes)}
              : std::make_shared<UnpooledChunksManager>(memoryResource, unpooledMemoryLimitInBytes))
    , bufferSize(bufferSize)
    , numOfBuffers(numOfBuffers)
    , alignment(alignment)
    , memoryResource(std::move(memoryResource))
{
    PRECONDITION(numOfBuffers > 0, "BufferManager requires at least one pooled buffer, but the configured budget yields {}", numOfBuffers);
    initialize();
}

std::shared_ptr<BufferManager> BufferManager::create(
    const size_t totalMemoryInBytes,
    const double unpooledMemoryFraction,
    const BufferAlignment alignment,
    const uint32_t bufferSize,
    const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
    const UnpooledBufferManagerType unpooledBufferManagerType)
{
    PRECONDITION(
        unpooledMemoryFraction >= 0.0 and unpooledMemoryFraction <= 1.0,
        "unpooledMemoryFraction must be in [0.0, 1.0] but is {}",
        unpooledMemoryFraction);
    PRECONDITION(bufferSize > 0, "bufferSize must be greater than zero");

    const auto unpooledMemoryLimitInBytes = static_cast<size_t>(static_cast<double>(totalMemoryInBytes) * unpooledMemoryFraction);
    const size_t pooledMemoryInBytes = totalMemoryInBytes - unpooledMemoryLimitInBytes;
    PRECONDITION(
        pooledMemoryInBytes / bufferSize <= std::numeric_limits<uint32_t>::max(),
        "pooled buffer count {} exceeds uint32_t; lower total_memory_in_bytes or raise buffer_size",
        pooledMemoryInBytes / bufferSize);
    const auto numOfBuffers = static_cast<uint32_t>(pooledMemoryInBytes / bufferSize);
    return std::make_shared<BufferManager>(
        Private{},
        bufferSize,
        numOfBuffers,
        memoryResource,
        unpooledMemoryLimitInBytes,
        alignment.getRawValue(),
        unpooledBufferManagerType);
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
        /// All buffers should be idle now, so their control blocks (and data regions) are poisoned. Clear all
        /// poison up front: the availability checks and the MemorySegment destructors in allBuffers.clear()
        /// below read the control blocks, and the arena is about to be returned to the allocator anyway.
        ASAN_UNPOISON_MEMORY_REGION(basePointer, allocatedAreaSize);
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
        memoryResource->deallocate(basePointer, allocatedAreaSize, alignment);
        allocatedAreaSize = 0;

        /// Destroying the unpooled chunks
        unpooledBufferManager.reset();
    }
}

void BufferManager::initialize()
{
    /// Buffer alignment is configured at construction
    const uint32_t withAlignment = alignment;
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
    const auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), withAlignment);
    const auto alignedBufferSize = alignBufferSize(bufferSize, withAlignment);

    /// Each buffer slot is laid out as:
    ///   [ redzone | control block | redzone | data region ]
    /// Under ASan the two redzones are permanently poisoned to trap any over-/underflow that would
    /// corrupt the control block. CONTROL_BLOCK_REDZONE_SIZE is 0 without ASan, so the slot size
    /// below collapses to the previous (controlBlockSize + alignedBufferSize) layout exactly.
    const auto redzoneSize = detail::CONTROL_BLOCK_REDZONE_SIZE;
    const size_t offsetBetweenBuffers = redzoneSize + controlBlockSize + redzoneSize + alignedBufferSize;
    /// One extra trailing redzone guards the data region of the very last slot.
    allocatedAreaSize = (offsetBetweenBuffers * numOfBuffers) + redzoneSize;
    basePointer = static_cast<uint8_t*>(memoryResource->allocate(allocatedAreaSize, withAlignment));
    INVARIANT(basePointer, "memory allocation failed, because 'basePointer' was a nullptr");

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

    uint8_t* ptr = basePointer;
    for (size_t i = 0; i < numOfBuffers; ++i)
    {
        /// Slot layout: [ redzone | control block | redzone | data region ]. redzoneSize is 0 without ASan.
        uint8_t* const controlBlock = ptr + redzoneSize;
        uint8_t* const payload = controlBlock + controlBlockSize + redzoneSize;
        allBuffers.emplace_back(
            payload,
            bufferSize,
            [](detail::MemorySegment* segment, BufferRecycler* recycler) { recycler->recyclePooledBuffer(segment); },
            controlBlock);

        availableBuffers.write(&allBuffers.back());
        ptr += offsetBetweenBuffers;
    }

    /// All buffers start idle, so poison the whole arena in one shot: redzones, control blocks and data
    /// regions alike. The redzones stay poisoned for the arena's lifetime; prepare() unpoisons a control
    /// block on hand-out and recyclePooledBuffer() re-poisons it, and the data region is likewise unpoisoned
    /// on hand-out and re-poisoned on recycle. This must run after the construction loop so that the
    /// placement-new of each control block (and the debug marker fill above) writes to unpoisoned memory.
    ASAN_POISON_MEMORY_REGION(basePointer, allocatedAreaSize);
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
        /// The buffer is now exclusively owned by the caller: unpoison its data region so it can be used.
        ASAN_UNPOISON_MEMORY_REGION(memSegment->ptr, memSegment->size);
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    detail::MemorySegment* memSegment = nullptr;
    if (!availableBuffers.read(memSegment))
    {
        static __itt_domain* bufferManagerDomain = __itt_domain_create("buffer-manager");
        static __itt_string_handle* waitForBuffer = __itt_string_handle_create("Wait for buffer");
        __itt_task_begin(bufferManagerDomain, __itt_null, __itt_null, waitForBuffer);
        const auto deadline = std::chrono::steady_clock::now() + timeoutMs;
        if (!availableBuffers.tryReadUntil(deadline, memSegment))
        {
            __itt_task_end(bufferManagerDomain);
            return std::nullopt;
        }
        __itt_task_end(bufferManagerDomain);
    }
    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        /// The buffer is now exclusively owned by the caller: unpoison its data region so it can be used.
        ASAN_UNPOISON_MEMORY_REGION(memSegment->ptr, memSegment->size);
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(const size_t bufferSize)
{
    return unpooledBufferManager->getUnpooledBuffer(bufferSize, alignment, shared_from_this());
}

void BufferManager::recyclePooledBuffer(detail::MemorySegment* segment)
{
    INVARIANT(segment->isAvailable(), "Recycling buffer callback invoked on used memory segment");
    INVARIANT(
        segment->controlBlock->owningBufferRecycler == nullptr, "Buffer should not retain a reference to its parent while not in use");
    /// The buffer is back in the pool and must not be touched until handed out again: re-poison its data
    /// region and its control block. This is the control block's last read on the recycle path (the INVARIANTs
    /// above), and prepare() unpoisons it again on the next hand-out. Poisoning before the enqueue
    /// happens-before that unpoison via the availableBuffers queue, so there is no shadow-memory race.
    ASAN_POISON_MEMORY_REGION(segment->ptr, segment->size);
    ASAN_POISON_MEMORY_REGION(segment->controlBlock.get(), sizeof(detail::BufferControlBlock));
    USED_IN_DEBUG const auto couldRecycleBuffer = availableBuffers.writeIfNotFull(segment);
    INVARIANT(couldRecycleBuffer, "should always succeed");
}

void BufferManager::recycleUnpooledBuffer(detail::MemorySegment*, const AllocationThreadInfo&)
{
    INVARIANT(false, "This method should not be called!");
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
    return unpooledBufferManager->getNumberOfUnpooledBuffers();
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
