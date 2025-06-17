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
#include <atomic>
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
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <ErrorHandling.hpp>
#include <FixedSizeBufferPool.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{

BufferManager::BufferManager(
    Private,
    const uint32_t bufferSize,
    const uint32_t numOfBuffers,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    const uint32_t withAlignment)
    : availableBuffers(numOfBuffers)
    , numOfAvailableBuffers(numOfBuffers)
    , unpooledChunksManager(memoryResource)
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
        const std::scoped_lock lock(availableBuffersMutex, localBufferPoolsMutex);
        auto success = true;
        NES_DEBUG("Shutting down Buffer Manager");

        /// Destroy all active localBufferPools
        for (auto& localPool : localBufferPools)
        {
            /// If the weak ptr is invalid the local buffer pool has already been destroyed
            if (const auto pool = localPool.lock())
            {
                pool->destroy();
            }
        }
        localBufferPools.clear();

        size_t numOfAvailableBuffers = this->numOfAvailableBuffers.load();

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
        NES_DEBUG("Shutting down Buffer Manager completed");
        memoryResource->deallocate(basePointer, allocatedAreaSize);
    }
}

std::shared_ptr<BufferManager> BufferManager::create(
    uint32_t bufferSize, uint32_t numOfBuffers, const std::shared_ptr<std::pmr::memory_resource>& memoryResource, uint32_t withAlignment)
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

    const size_t pages = sysconf(_SC_PHYS_PAGES);
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
    const size_t offsetBetweenBuffers = allocatedAreaSize;
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
    numOfAvailableBuffers.fetch_sub(1);
    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    detail::MemorySegment* memSegment = nullptr;
    const auto deadline = std::chrono::steady_clock::now() + timeoutMs;
    if (!availableBuffers.tryReadUntil(deadline, memSegment))
    {
        return std::nullopt;
    }
    numOfAvailableBuffers.fetch_sub(1);
    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(const size_t bufferSize)
{
    return unpooledChunksManager.getUnpooledBuffer(bufferSize, DEFAULT_ALIGNMENT, shared_from_this());
}

void BufferManager::recyclePooledBuffer(detail::MemorySegment* segment)
{
    INVARIANT(segment->isAvailable(), "Recycling buffer callback invoked on used memory segment");
    INVARIANT(
        segment->controlBlock->owningBufferRecycler == nullptr, "Buffer should not retain a reference to its parent while not in use");
    availableBuffers.write(segment);
    numOfAvailableBuffers.fetch_add(1);
}

void BufferManager::recycleUnpooledBuffer(detail::MemorySegment*)
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
    return unpooledChunksManager.getNumberOfUnpooledBuffers();
}

size_t BufferManager::getTotalSizeOfUnpooledBufferChunks() const
{
    return unpooledChunksManager.getTotalSizeOfUnpooledBufferChunks();
}

size_t BufferManager::getAvailableBuffers() const
{
    return numOfAvailableBuffers.load();
}

size_t BufferManager::getAvailableBuffersInFixedSizePools() const
{
    const std::unique_lock lock(localBufferPoolsMutex);
    const auto numberOfAvailableBuffers = std::ranges::count_if(localBufferPools, [](auto& weak) { return !weak.expired(); });
    return numberOfAvailableBuffers;
}

BufferManagerType BufferManager::getBufferManagerType() const
{
    return BufferManagerType::GLOBAL;
}

std::optional<std::shared_ptr<AbstractBufferProvider>> BufferManager::createFixedSizeBufferPool(size_t numberOfReservedBuffers)
{
    const std::unique_lock lock(availableBuffersMutex);
    std::deque<detail::MemorySegment*> buffers;

    NES_DEBUG(
        "Attempting to create fixed size buffer pool of {} Buffers. Currently available are {} buffers",
        numberOfReservedBuffers,
        numOfAvailableBuffers);

    const ssize_t numberOfAvailableBuffers = availableBuffers.size();
    if (numberOfAvailableBuffers < 0 || static_cast<size_t>(numberOfAvailableBuffers) < numberOfReservedBuffers)
    {
        NES_WARNING("Could not allocate FixedSizeBufferPool with {} buffers", numberOfReservedBuffers);
        return {};
    }

    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i)
    {
        detail::MemorySegment* memorySegment = nullptr;
        availableBuffers.blockingRead(memorySegment);
        numOfAvailableBuffers.fetch_sub(1);
        buffers.emplace_back(memorySegment);
    }
    auto ret = std::make_shared<FixedSizeBufferPool>(shared_from_this(), buffers, numberOfReservedBuffers);
    {
        std::unique_lock lock(localBufferPoolsMutex);
        localBufferPools.push_back(ret);
    }
    return ret;
}

}
