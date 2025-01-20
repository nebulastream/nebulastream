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

#include <cstring>
#include <deque>
#include <iostream>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <FixedSizeBufferPool.hpp>
#include <LocalBufferPool.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{

BufferManager::BufferManager(
    Private, uint32_t bufferSize, uint32_t numOfBuffers, std::shared_ptr<std::pmr::memory_resource> memoryResource, uint32_t withAlignment)
    : availableBuffers(numOfBuffers)
    , numOfAvailableBuffers(numOfBuffers)
    , bufferSize(bufferSize)
    , numOfBuffers(numOfBuffers)
    , memoryResource(std::move(memoryResource))
{
    ((void)withAlignment);
    initialize(DEFAULT_ALIGNMENT);
}

void BufferManager::destroy()
{
    bool expected = false;
    if (isDestroyed.compare_exchange_strong(expected, true))
    {
        std::scoped_lock lock(availableBuffersMutex, unpooledBuffersMutex, localBufferPoolsMutex);
        auto success = true;
        NES_DEBUG("Shutting down Buffer Manager");
        for (auto& localPool : localBufferPools)
        {
            localPool->destroy();
        }
        size_t numOfAvailableBuffers = this->numOfAvailableBuffers.load();

        localBufferPools.clear();
        if (allBuffers.size() != numOfAvailableBuffers)
        {
            NES_ERROR("[BufferManager] total buffers {} :: available buffers {}", allBuffers.size(), numOfAvailableBuffers);
            success = false;
        }
        for (auto& buffer : allBuffers)
        {
            if (!buffer.isAvailable())
            {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
                buffer.controlBlock->dumpOwningThreadInfo();
#endif
                success = false;
            }
        }
        INVARIANT(
            success,
            "Requested buffer manager shutdown but a buffer is still used allBuffers={} available={}",
            allBuffers.size(),
            numOfAvailableBuffers);
        /// RAII takes care of deallocating memory here
        allBuffers.clear();

        availableBuffers = decltype(availableBuffers)();
        for (auto& holder : unpooledBuffers)
        {
            if (!holder.segment || holder.segment->controlBlock->getReferenceCount() != 0)
            {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
                if (holder.segment)
                {
                    holder.segment->controlBlock->dumpOwningThreadInfo();
                }
#endif
                INVARIANT(
                    false,
                    "Deletion of unpooled buffer invoked on used memory segment size={} refcnt={}",
                    holder.size,
                    holder.segment->controlBlock->getReferenceCount());
            }
        }
        unpooledBuffers.clear();
        NES_DEBUG("Shutting down Buffer Manager completed");
        memoryResource->deallocate(basePointer, allocatedAreaSize);
    }
}

std::shared_ptr<BufferManager> BufferManager::create(
    uint32_t bufferSize, uint32_t numOfBuffers, std::shared_ptr<std::pmr::memory_resource> memoryResource, uint32_t withAlignment)
{
    return std::make_shared<BufferManager>(Private{}, bufferSize, numOfBuffers, memoryResource, withAlignment);
}

BufferManager::~BufferManager()
{
    BufferManager::destroy();
}

void BufferManager::initialize(uint32_t withAlignment)
{
    std::unique_lock lock(availableBuffersMutex);

    size_t pages = sysconf(_SC_PHYS_PAGES);
    size_t page_size = sysconf(_SC_PAGE_SIZE);
    auto memorySizeInBytes = pages * page_size;

    uint64_t requiredMemorySpace = this->bufferSize * this->numOfBuffers;
    double percentage = (100.0 * requiredMemorySpace) / memorySizeInBytes;
    NES_DEBUG("NES memory allocation requires {} out of {} (so {}%) available bytes", requiredMemorySpace, memorySizeInBytes, percentage);

    ///    NES_ASSERT2_FMT(bufferSize && !(bufferSize & (bufferSize - 1)), "size must be power of two " << bufferSize);
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
    size_t offsetBetweenBuffers = allocatedAreaSize;
    allocatedAreaSize *= numOfBuffers;
    basePointer = static_cast<uint8_t*>(memoryResource->allocate(allocatedAreaSize, withAlignment));
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
            this,
            [](detail::MemorySegment* segment, BufferRecycler* recycler) { recycler->recyclePooledBuffer(segment); },
            controlBlock);

        availableBuffers.write(&allBuffers.back());
        ptr += offsetBetweenBuffers;
    }
    NES_DEBUG("BufferManager configuration bufferSize={} numOfBuffers={}", this->bufferSize, this->numOfBuffers);
}

TupleBuffer BufferManager::getBufferBlocking()
{
    detail::MemorySegment* memSegment;
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
    numOfAvailableBuffers.fetch_sub(1);
    if (memSegment->controlBlock->prepare())
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(std::chrono::milliseconds timeout_ms)
{
    detail::MemorySegment* memSegment;
    auto deadline = std::chrono::steady_clock::now() + timeout_ms;
    if (!availableBuffers.tryReadUntil(deadline, memSegment))
    {
        return std::nullopt;
    }
    numOfAvailableBuffers.fetch_sub(1);
    if (memSegment->controlBlock->prepare())
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(size_t bufferSize)
{
    std::unique_lock lock(unpooledBuffersMutex);
    UnpooledBufferHolder probe(bufferSize);
    auto candidate = std::lower_bound(unpooledBuffers.begin(), unpooledBuffers.end(), probe);
    if (candidate != unpooledBuffers.end())
    {
        /// it points to a segment of size at least bufferSize;
        for (auto it = candidate; it != unpooledBuffers.end(); ++it)
        {
            if (it->size == bufferSize)
            {
                if (it->free)
                {
                    auto* memSegment = it->segment.get();
                    it->free = false;
                    if (memSegment->controlBlock->prepare())
                    {
                        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
                    }
                    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
                }
            }
            else
            {
                break;
            }
        }
    }
    /// we could not find a buffer, allocate it
    /// we have to align the buffer size as ARM throws an SIGBUS if we have unaligned accesses on atomics.
    auto alignedBufferSize = alignBufferSize(bufferSize, DEFAULT_ALIGNMENT);
    auto alignedBufferSizePlusControlBlock = alignBufferSize(bufferSize + sizeof(detail::BufferControlBlock), DEFAULT_ALIGNMENT);
    auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), DEFAULT_ALIGNMENT);
    auto* ptr = static_cast<uint8_t*>(memoryResource->allocate(alignedBufferSizePlusControlBlock, DEFAULT_ALIGNMENT));
    INVARIANT(ptr, "Unpooled memory allocation failed");
    NES_TRACE(
        "Ptr: {} alignedBufferSize: {} alignedBufferSizePlusControlBlock: {} controlBlockSize: {}",
        reinterpret_cast<uintptr_t>(ptr),
        alignedBufferSize,
        alignedBufferSizePlusControlBlock,
        controlBlockSize);
    auto memSegment = std::make_unique<detail::MemorySegment>(
        ptr + controlBlockSize,
        alignedBufferSize,
        this,
        [](detail::MemorySegment* segment, BufferRecycler* recycler) { recycler->recycleUnpooledBuffer(segment); },
        ptr);
    auto* leakedMemSegment = memSegment.get();
    unpooledBuffers.emplace_back(std::move(memSegment), alignedBufferSize);
    if (leakedMemSegment->controlBlock->prepare())
    {
        return TupleBuffer(leakedMemSegment->controlBlock.get(), leakedMemSegment->ptr, bufferSize);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

void BufferManager::recyclePooledBuffer(detail::MemorySegment* segment)
{
    INVARIANT(segment->isAvailable(), "Recycling buffer callback invoked on used memory segment");
    availableBuffers.write(segment);
    numOfAvailableBuffers.fetch_add(1);
}

void BufferManager::recycleUnpooledBuffer(detail::MemorySegment* segment)
{
    std::unique_lock lock(unpooledBuffersMutex);
    INVARIANT(segment->isAvailable(), "Recycling buffer callback invoked on used memory segment");
    UnpooledBufferHolder probe(segment->getSize());
    auto candidate = std::lower_bound(unpooledBuffers.begin(), unpooledBuffers.end(), probe);
    if (candidate != unpooledBuffers.end())
    {
        for (auto it = candidate; it != unpooledBuffers.end(); ++it)
        {
            if (it->size == probe.size)
            {
                if (it->segment->ptr == segment->ptr)
                {
                    it->markFree();
                    return;
                }
            }
            else
            {
                break;
            }
        }
    }
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
    std::unique_lock lock(unpooledBuffersMutex);
    return unpooledBuffers.size();
}

size_t BufferManager::getAvailableBuffers() const
{
    return numOfAvailableBuffers.load();
}

size_t BufferManager::getAvailableBuffersInFixedSizePools() const
{
    std::unique_lock lock(localBufferPoolsMutex);
    size_t sum = 0;
    for (auto& pool : localBufferPools)
    {
        auto type = pool->getBufferManagerType();
        if (type == BufferManagerType::FIXED)
        {
            sum += pool->getAvailableBuffers();
        }
    }
    return sum;
}

BufferManagerType BufferManager::getBufferManagerType() const
{
    return BufferManagerType::GLOBAL;
}

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder()
{
    segment.reset();
}

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder(uint32_t bufferSize) : size(bufferSize), free(false)
{
    segment.reset();
}

BufferManager::UnpooledBufferHolder::UnpooledBufferHolder(std::unique_ptr<detail::MemorySegment>&& mem, uint32_t size)
    : segment(std::move(mem)), size(size), free(false)
{
    /// nop
}

void BufferManager::UnpooledBufferHolder::markFree()
{
    free = true;
}

std::optional<std::shared_ptr<AbstractBufferProvider>> BufferManager::createLocalBufferPool(size_t numberOfReservedBuffers)
{
    std::unique_lock lock(availableBuffersMutex);
    std::deque<detail::MemorySegment*> buffers;
    NES_DEBUG("availableBuffers.size()={} requested buffers={}", availableBuffers.size(), numberOfReservedBuffers);

    const ssize_t numberOfAvailableBuffers = availableBuffers.size();
    if (numberOfAvailableBuffers < 0 || static_cast<size_t>(numberOfAvailableBuffers) < numberOfReservedBuffers)
    {
        NES_WARNING("Could not allocate LocalBufferPool with {} buffers", numberOfReservedBuffers);
        return {};
    }

    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i)
    {
        detail::MemorySegment* memorySegment;
        availableBuffers.blockingRead(memorySegment);
        numOfAvailableBuffers.fetch_sub(1);
        buffers.emplace_back(memorySegment);
    }
    auto ret = std::make_shared<LocalBufferPool>(shared_from_this(), std::move(buffers), numberOfReservedBuffers);
    {
        std::unique_lock lock(localBufferPoolsMutex);
        localBufferPools.push_back(ret);
    }
    return ret;
}

std::optional<std::shared_ptr<AbstractBufferProvider>> BufferManager::createFixedSizeBufferPool(size_t numberOfReservedBuffers)
{
    std::unique_lock lock(availableBuffersMutex);
    std::deque<detail::MemorySegment*> buffers;

    const ssize_t numberOfAvailableBuffers = availableBuffers.size();
    if (numberOfAvailableBuffers < 0 || static_cast<size_t>(numberOfAvailableBuffers) < numberOfReservedBuffers)
    {
        NES_WARNING("Could not allocate FixedSizeBufferPool with {} buffers", numberOfReservedBuffers);
        return {};
    }

    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i)
    {
        detail::MemorySegment* memorySegment;
        availableBuffers.blockingRead(memorySegment);
        numOfAvailableBuffers.fetch_sub(1);
        buffers.emplace_back(memorySegment);
    }
    auto ret = std::make_shared<FixedSizeBufferPool>(shared_from_this(), std::move(buffers), numberOfReservedBuffers);
    {
        std::unique_lock lock(localBufferPoolsMutex);
        localBufferPools.push_back(ret);
    }
    return ret;
}

TupleBuffer allocateVariableLengthField(std::shared_ptr<AbstractBufferProvider> provider, uint32_t size)
{
    auto optBuffer = provider->getUnpooledBuffer(size);
    INVARIANT(!!optBuffer, "Cannot allocate buffer of size {}", size);
    return *optBuffer;
}

}
