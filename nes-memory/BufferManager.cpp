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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <sys/types.h>
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
    , rollingAverage(RollingAverage<size_t>(ROLLING_AVERAGE_UNPOOLED_BUFFER_SIZE))
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
            if (auto pool = localPool.lock())
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
            [](detail::MemorySegment* segment, BufferRecycler* recycler) { recycler->recyclePooledBuffer(segment); },
            controlBlock);

        availableBuffers.write(&allBuffers.back());
        ptr += offsetBetweenBuffers;
    }
    NES_DEBUG("BufferManager configuration bufferSize={} numOfBuffers={}", this->bufferSize, this->numOfBuffers);

    {
        /// Creating here the first chunk for unpooled buffer with a buffer size
        /// we have to align the buffer size as ARM throws an SIGBUS if we have unaligned accesses on atomics.
        std::scoped_lock unpooledBuffersLock(unpooledBuffersMutex);
        const auto alignedBufferSizePlusControlBlock = alignBufferSize(bufferSize + sizeof(detail::BufferControlBlock), DEFAULT_ALIGNMENT);
        const auto newAllocationSize = alignedBufferSizePlusControlBlock * NUM_PRE_ALLOCATED_CHUNKS;
        auto ptr = static_cast<uint8_t*>(memoryResource->allocate(newAllocationSize, DEFAULT_ALIGNMENT));
        INVARIANT(ptr, "Unpooled memory allocation failed");
        lastAllocateChunkPtr = ptr;
        auto& currentAllocatedChunk = unpooledBufferChunkStorage[lastAllocateChunkPtr];
        currentAllocatedChunk.totalSize = newAllocationSize;
        currentAllocatedChunk.startOfChunk = ptr;
    }
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
    /// we have to align the buffer size as ARM throws an SIGBUS if we have unaligned accesses on atomics.
    const auto alignedBufferSizePlusControlBlock = alignBufferSize(bufferSize + sizeof(detail::BufferControlBlock), DEFAULT_ALIGNMENT);
    const auto newRollingAverage = rollingAverage.wlock()->add(alignedBufferSizePlusControlBlock);

    /// To reduce the amount of time we will held a lock, we first collect all necessary information to create and emplace a new memory segment
    /// in the unpooled buffer chunk storage
    uint8_t* memoryForNewTupleBuffer = nullptr;
    uint8_t* keyForUnpooledBufferChunk = nullptr;


    /// Trying if there is enough space in the last allocated chunk.
    /// We perform this check in a new block so that we only lock for the check.
    /// We use here a lambda that we invoke immediatly to give us the option to return
    [&]()
    {
        {
            std::scoped_lock lock(unpooledBuffersMutex);
            if (auto& currentAllocatedChunk = unpooledBufferChunkStorage[lastAllocateChunkPtr];
                currentAllocatedChunk.usedSize + alignedBufferSizePlusControlBlock < currentAllocatedChunk.totalSize)
            {
                /// There is enough space in the last allocated chunk. Thus, we can create a tuple buffer from the available space
                memoryForNewTupleBuffer = currentAllocatedChunk.startOfChunk + currentAllocatedChunk.usedSize;
                keyForUnpooledBufferChunk = lastAllocateChunkPtr;
                currentAllocatedChunk.activeMemorySegments += 1;
                currentAllocatedChunk.usedSize += alignedBufferSizePlusControlBlock;
                return;
            }
        }
        /// The last allocated chunk is not enough. Thus, we need to allocate a new chunk and insert it into the unpooled buffer storage
        /// The memory to allocate must be larger than bufferSize, while also taking the rolling average into account.
        /// For now, we allocate multiple rolling averages. If this is too small for the current bufferSize, we allocate the bufferSize
        const auto newAllocationSize
            = std::max(static_cast<const unsigned long>(alignedBufferSizePlusControlBlock), newRollingAverage * NUM_PRE_ALLOCATED_CHUNKS);
        const auto newlyAllocatedMemory = static_cast<uint8_t*>(memoryResource->allocate(newAllocationSize, DEFAULT_ALIGNMENT));
        std::memset(newlyAllocatedMemory, 0, newAllocationSize);
        INVARIANT(newlyAllocatedMemory, "Unpooled memory allocation failed");

        {
            /// It might happen that in the mean time, the another thread also has created a new memory and now multiple threads want to
            /// add their newly allocated memory into the unpooledBufferChunkStorage.
            std::unique_lock lock(unpooledBuffersMutex);
            if (unpooledBufferChunkStorage.contains(lastAllocateChunkPtr)
                and unpooledBufferChunkStorage[lastAllocateChunkPtr].usedSize + alignedBufferSizePlusControlBlock
                    < unpooledBufferChunkStorage[lastAllocateChunkPtr].totalSize)
            {
                /// Another thread has won and there is enough space on the newly chunk (created from another thread)
                /// Therefore, we use the already existing chunk
                auto& existingChunk = unpooledBufferChunkStorage[lastAllocateChunkPtr];
                memoryForNewTupleBuffer = existingChunk.startOfChunk + existingChunk.usedSize;
                keyForUnpooledBufferChunk = lastAllocateChunkPtr;
                existingChunk.activeMemorySegments += 1;
                existingChunk.usedSize += alignedBufferSizePlusControlBlock;

                /// As we have already allocated memory, we check if we can use the allocated memory.
                /// We define if we can use the memory, if the free space on the current chunk is less than the calcualted rolling average
                const auto freeSpace = existingChunk.totalSize - existingChunk.usedSize;
                if (freeSpace < newRollingAverage)
                {
                    /// The space is not large enough for another unpooled buffer. Therefore, we add the already allocated one to the unpooled chunk storage
                    lastAllocateChunkPtr = newlyAllocatedMemory;
                    unpooledBufferChunkStorage[lastAllocateChunkPtr].startOfChunk = newlyAllocatedMemory;
                    unpooledBufferChunkStorage[lastAllocateChunkPtr].totalSize = newAllocationSize;
                }
                else
                {
                    /// We do not need the allocated memory and thus, we have to to deallocate the created memory but do not need the lock for this
                    lock.unlock();
                    memoryResource->deallocate(newlyAllocatedMemory, newAllocationSize, DEFAULT_ALIGNMENT);
                }
            }
            else
            {
                /// Either no thread has already emplaced its allocated memory or the allocated space (from another thread) is already full
                lastAllocateChunkPtr = newlyAllocatedMemory;
                keyForUnpooledBufferChunk = newlyAllocatedMemory;
                memoryForNewTupleBuffer = newlyAllocatedMemory;
                unpooledBufferChunkStorage[lastAllocateChunkPtr].startOfChunk = newlyAllocatedMemory;
                unpooledBufferChunkStorage[lastAllocateChunkPtr].totalSize = newAllocationSize;
                unpooledBufferChunkStorage[lastAllocateChunkPtr].usedSize += alignedBufferSizePlusControlBlock;
                unpooledBufferChunkStorage[lastAllocateChunkPtr].activeMemorySegments += 1;
            }
        }
    }();

    /// Creating a new memory segment, adding it to the unpooledMemorySegments
    const auto alignedBufferSize = alignBufferSize(bufferSize, DEFAULT_ALIGNMENT);
    constexpr auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), DEFAULT_ALIGNMENT);
    auto memSegment = std::make_unique<detail::MemorySegment>(
        memoryForNewTupleBuffer + controlBlockSize,
        alignedBufferSize,
        [this, copyOLastChunkPtr = keyForUnpooledBufferChunk](detail::MemorySegment* memorySegment, BufferRecycler*)
        {
            std::unique_lock lock(unpooledBuffersMutex);
            auto& curUnpooledChunk = unpooledBufferChunkStorage[copyOLastChunkPtr];
            INVARIANT(
                curUnpooledChunk.activeMemorySegments > 0,
                "curUnpooledChunk.activeMemorySegments must be larger than 0 but is {}",
                curUnpooledChunk.activeMemorySegments);
            curUnpooledChunk.activeMemorySegments -= 1;
            memorySegment->size = 0;
            if (curUnpooledChunk.activeMemorySegments == 0)
            {
                /// All memory segments have been removed, therefore, we can deallocate the unpooled chunk
                auto extractedChunk = unpooledBufferChunkStorage.extract(copyOLastChunkPtr);
                unpooledBufferChunkStorage.erase(copyOLastChunkPtr);
                lock.unlock();
                memoryResource->deallocate(curUnpooledChunk.startOfChunk, curUnpooledChunk.totalSize, DEFAULT_ALIGNMENT);
            }
        });
    auto* leakedMemSegment = memSegment.get();
    {
        /// Inserting the memory segment into the unpooled buffer storage
        std::scoped_lock lock(unpooledBuffersMutex);
        auto& curUnpooledChunk = unpooledBufferChunkStorage[keyForUnpooledBufferChunk];
        curUnpooledChunk.unpooledMemorySegments.emplace_back(std::move(memSegment));
    }

    if (leakedMemSegment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(leakedMemSegment->controlBlock.get(), leakedMemSegment->ptr, bufferSize);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
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
    std::scoped_lock lock(unpooledBuffersMutex);
    uint64_t numOfUnpooledBuffers = 0;
    std::ranges::for_each(
        unpooledBufferChunkStorage,
        [&numOfUnpooledBuffers](const auto& pair) { numOfUnpooledBuffers += pair.second.activeMemorySegments; });
    return numOfUnpooledBuffers;
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

TupleBuffer allocateVariableLengthField(std::shared_ptr<AbstractBufferProvider> provider, uint32_t size)
{
    auto optBuffer = provider->getUnpooledBuffer(size);
    INVARIANT(!!optBuffer, "Cannot allocate buffer of size {}", size);
    return *optBuffer;
}

}
