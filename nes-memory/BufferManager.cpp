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

#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <memory>
#include <memory_resource>
#include <optional>
#include <ranges>
#include <span>
#include <stop_token>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <FixedSizeBufferPool.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::Memory
{

BufferManager::BufferManager(
    Private,
    std::span<std::byte> memoryAllocation,
    std::vector<detail::MemorySegment> allBuffers,
    const uint32_t bufferSize,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    std::vector<detail::MemorySegment*> availableBuffers)
    : memoryResource(std::move(memoryResource))
    , memoryAllocation(memoryAllocation)
    , bufferSize(bufferSize)
    , allBuffers(std::move(allBuffers))
    , availableBuffers(std::move(availableBuffers))
{
}

void BufferManager::destroy()
{
    auto destroyLock = isDestroyed.wlock();
    if (*destroyLock)
    {
        return;
    }

    {
        /// Destroy all local buffers
        auto localPoolsLocked = localBufferPools.wlock();
        for (auto& localPool : *localPoolsLocked)
        {
            /// If the weak ptr is invalid the local buffer pool has already been destroyed
            if (auto pool = localPool.lock())
            {
                pool->destroy();
            }
        }
        localPoolsLocked->clear();
    }

    {
        /// Wait for allBuffers
        auto availableBuffersLocked = availableBuffers.lock();
        if (availableBuffersLocked->size() != allBuffers.rlock()->size())
        {
            auto allBuffersReleased = bufferAvailableCondition.wait_for(
                availableBuffersLocked.as_lock(),
                std::chrono::seconds(1),
                [this, &availableBuffersLocked]() { return availableBuffersLocked->size() == allBuffers.rlock()->size(); });

            if (!allBuffersReleased)
            {
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
                NES_ERROR("Dumping all leaked buffers:")
                for (auto* buffer : allBuffers)
                {
                    if (!buffer->isAvailable())
                    {
                        buffer.controlBlock->dumpOwningThreadInfo();
                    }
                }
#endif
                NES_FATAL_ERROR("BufferManager::destroy timed out waiting for all buffers to be released");
            }
            INVARIANT(allBuffersReleased, "BufferManager::destroy timed out waiting for all buffers to be released");
        }
    }

    allBuffers.wlock()->clear();
    memoryResource->deallocate(memoryAllocation.data(), memoryAllocation.size());
    unpooledBuffers.wlock()->clear();
    *destroyLock = true;
}

namespace
{
std::pair<std::span<std::byte>, std::vector<detail::MemorySegment>> initialize(
    size_t bufferSize, size_t numberOfBuffers, size_t withAlignment, const std::shared_ptr<std::pmr::memory_resource>& memoryResource)
{
    PRECONDITION(bufferSize && !(bufferSize & (bufferSize - 1)), "size must be power of two got {}", bufferSize);
    PRECONDITION(
        withAlignment > 0 && !(withAlignment & (withAlignment - 1)),
        "NES tries to align memory but alignment={} is not a pow of two",
        withAlignment);
    PRECONDITION(
        alignof(detail::BufferControlBlock) <= withAlignment,
        "Requested alignment is too small, must be at least {}",
        alignof(detail::BufferControlBlock));

    const size_t pages = sysconf(_SC_PHYS_PAGES);
    const size_t pageSize = sysconf(_SC_PAGE_SIZE);
    if (withAlignment > pageSize)
    {
        throw BufferAllocationFailure("NES tries to align memory but alignment is invalid: {} ", withAlignment);
    }

    auto memorySizeInBytes = pages * pageSize;
    uint64_t requiredMemorySpace = bufferSize * numberOfBuffers;
    double percentage = 100.0 * static_cast<double>(requiredMemorySpace) / static_cast<double>(memorySizeInBytes);
    NES_DEBUG("NES memory allocation requires {} out of {} (so {}%) available bytes", requiredMemorySpace, memorySizeInBytes, percentage);

    if (requiredMemorySpace > memorySizeInBytes)
    {
        throw BufferAllocationFailure(
            "NES tries to allocate more memory than physically available requested={} available={}",
            requiredMemorySpace,
            memorySizeInBytes);
    }

    auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), withAlignment);
    auto alignedBufferSize = alignBufferSize(bufferSize, withAlignment);
    auto allocatedAreaSize = alignBufferSize(controlBlockSize + alignedBufferSize, withAlignment);
    const size_t offsetBetweenBuffers = allocatedAreaSize;
    allocatedAreaSize *= numberOfBuffers;

    auto memoryAllocation
        = std::span(static_cast<std::byte*>(memoryResource->allocate(allocatedAreaSize, withAlignment)), allocatedAreaSize);

    NES_DEBUG(
        "Allocated {} bytes with alignment {} buffer size {} num buffer {} controlBlockSize {} {}",
        allocatedAreaSize,
        withAlignment,
        alignedBufferSize,
        numberOfBuffers,
        controlBlockSize,
        alignof(detail::BufferControlBlock));

    if (memoryAllocation.data() == nullptr)
    {
        throw BufferAllocationFailure("NES could not allocate memory for buffer manager");
    }

    std::vector<detail::MemorySegment> allBuffers;
    allBuffers.reserve(numberOfBuffers);

    for (size_t i = 0; i < numberOfBuffers; ++i)
    {
        auto memorySegment = memoryAllocation.subspan(i * offsetBetweenBuffers, offsetBetweenBuffers);
        std::byte* controlBlock = memorySegment.data();
        std::byte* payload = memorySegment.subspan(controlBlockSize).data();
        allBuffers.emplace_back(
            std::bit_cast<uint8_t*>(payload),
            static_cast<uint32_t>(bufferSize),
            [](detail::MemorySegment* segment, BufferRecycler* recycler) { recycler->recyclePooledBuffer(segment); },
            std::bit_cast<uint8_t*>(controlBlock));
    }
    NES_DEBUG("BufferManager configuration bufferSize={} numberOfBuffers={}", bufferSize, numberOfBuffers);

    return {memoryAllocation, std::move(allBuffers)};
}
}

std::shared_ptr<BufferManager> BufferManager::create(
    uint32_t bufferSize, uint32_t numOfBuffers, std::shared_ptr<std::pmr::memory_resource> memoryResource, uint32_t withAlignment)
{
    auto [allocation, buffers] = initialize(bufferSize, numOfBuffers, withAlignment, memoryResource);
    auto availableBuffers = std::views::transform(buffers, [](auto& buffer) { return &buffer; }) | std::ranges::to<std::vector>();
    return std::make_shared<BufferManager>(
        Private{}, allocation, std::move(buffers), bufferSize, std::move(memoryResource), availableBuffers);
}

BufferManager::~BufferManager()
{
    BufferManager::destroy();
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
    auto segment = [&]() -> std::optional<detail::MemorySegment*>
    {
        auto buffersLocked = availableBuffers.lock();
        if (buffersLocked->empty())
        {
            return {};
        }
        auto* buffer = buffersLocked->back();
        buffersLocked->pop_back();
        return buffer;
    }();

    return segment.transform(
        [this](detail::MemorySegment* memSegment)
        {
            if (memSegment->controlBlock->prepare(shared_from_this()))
            {
                return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
            }
            throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
        });
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    auto segment = [&]() -> std::optional<detail::MemorySegment*>
    {
        auto buffersLocked = availableBuffers.lock();
        if (buffersLocked->empty())
        {
            auto hasBuffer = bufferAvailableCondition.wait_for(
                buffersLocked.as_lock(), timeoutMs, [&buffersLocked]() { return !buffersLocked->empty(); });
            if (!hasBuffer)
            {
                return {};
            }
        }
        auto* buffer = buffersLocked->back();
        buffersLocked->pop_back();
        return buffer;
    }();

    return segment.transform(
        [this](detail::MemorySegment* memSegment)
        {
            if (memSegment->controlBlock->prepare(shared_from_this()))
            {
                return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
            }
            throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
        });
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(const size_t bufferSize)
{
    /// we have to align the buffer size as ARM throws an SIGBUS if we have unaligned accesses on atomics.
    auto alignedBufferSize = alignBufferSize(bufferSize, DEFAULT_ALIGNMENT);
    auto alignedBufferSizePlusControlBlock = alignBufferSize(bufferSize + sizeof(detail::BufferControlBlock), DEFAULT_ALIGNMENT);
    auto controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), DEFAULT_ALIGNMENT);
    const std::span memoryAllocation(
        static_cast<std::byte*>(memoryResource->allocate(alignedBufferSizePlusControlBlock, DEFAULT_ALIGNMENT)),
        alignedBufferSizePlusControlBlock);

    INVARIANT(memoryAllocation.data(), "Unpooled memory allocation failed");
    NES_TRACE(
        "Ptr: {} alignedBufferSize: {} alignedBufferSizePlusControlBlock: {} controlBlockSize: {}",
        fmt::ptr(memoryAllocation.data()),
        alignedBufferSize,
        alignedBufferSizePlusControlBlock,
        controlBlockSize);

    auto memSegment = std::make_unique<detail::MemorySegment>(
        std::bit_cast<uint8_t*>(memoryAllocation.subspan(controlBlockSize).data()),
        alignedBufferSize,
        [this, memoryAllocation](detail::MemorySegment* memorySegment, BufferRecycler*)
        {
            auto unpooledBuffersLock = unpooledBuffers.wlock();
            auto segment = std::move(unpooledBuffersLock->at(memoryAllocation.data()));
            unpooledBuffersLock->erase(memoryAllocation.data());
            unpooledBuffersLock.unlock();

            memoryResource->deallocate(memoryAllocation.data(), memoryAllocation.size(), DEFAULT_ALIGNMENT);
            memorySegment->size = 0;
            /// segment dtor
        });
    auto* leakedMemSegment = memSegment.get();

    auto inserted = unpooledBuffers.wlock()->try_emplace(memoryAllocation.data(), std::move(memSegment));
    INVARIANT(inserted.second, "unpooledBuffer already contained a memory segment ptr to {}", fmt::ptr(leakedMemSegment->ptr));

    if (leakedMemSegment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(leakedMemSegment->controlBlock.get(), leakedMemSegment->ptr, bufferSize);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}


size_t BufferManager::getAvailableBuffers() const
{
    return availableBuffers.lock()->size();
}
size_t BufferManager::getNumOfPooledBuffers() const
{
    return allBuffers.rlock()->size();
}
size_t BufferManager::getNumOfUnpooledBuffers() const
{
    return unpooledBuffers.rlock()->size();
}
void BufferManager::recyclePooledBuffer(detail::MemorySegment* segment)
{
    INVARIANT(segment->isAvailable(), "Recycling buffer callback invoked on used memory segment");
    INVARIANT(
        segment->controlBlock->owningBufferRecycler == nullptr, "Buffer should not retain a reference to its parent while not in use");
    availableBuffers.lock()->emplace_back(segment);
    bufferAvailableCondition.notify_one();
}

void BufferManager::recycleUnpooledBuffer(detail::MemorySegment*)
{
    INVARIANT(false, "This method should not be called!");
}

size_t BufferManager::getBufferSize() const
{
    return bufferSize;
}

BufferManagerType BufferManager::getBufferManagerType() const
{
    return BufferManagerType::GLOBAL;
}

std::optional<std::shared_ptr<AbstractBufferProvider>>
BufferManager::createFixedSizeBufferPool(size_t numberOfReservedBuffers, std::chrono::seconds timeout)
{
    std::deque<detail::MemorySegment*> buffers;
    auto availableBuffersLocked = availableBuffers.lock();
    NES_DEBUG(
        "Attempting to create fixed size buffer pool of {} Buffers. Currently available are {} buffers",
        numberOfReservedBuffers,
        availableBuffersLocked->size());

    if (availableBuffersLocked->size() < numberOfReservedBuffers)
    {
        auto condition
            = [&availableBuffersLocked, numberOfReservedBuffers]() { return availableBuffersLocked->size() >= numberOfReservedBuffers; };
        auto bufferAvailable = bufferAvailableCondition.wait_for(availableBuffersLocked.as_lock(), timeout, condition);
        if (!bufferAvailable)
        {
            NES_WARNING("Could not allocate a fixed size buffer with {} buffers within {}.", numberOfReservedBuffers, timeout);
            return {};
        }
    }

    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i)
    {
        buffers.emplace_back(availableBuffersLocked->back());
        availableBuffersLocked->pop_back();
    }

    auto localPool = std::make_shared<FixedSizeBufferPool>(shared_from_this(), buffers, numberOfReservedBuffers);
    localBufferPools.wlock()->emplace_back(localPool);
    return localPool;
}

std::optional<std::shared_ptr<AbstractBufferProvider>> BufferManager::createFixedSizeBufferPool(size_t numberOfReservedBuffers)
{
    std::deque<detail::MemorySegment*> buffers;
    auto availableBuffersLocked = availableBuffers.lock();
    NES_DEBUG(
        "Attempting to create fixed size buffer pool of {} Buffers. Currently available are {} buffers",
        numberOfReservedBuffers,
        availableBuffersLocked->size());

    if (availableBuffersLocked->size() < numberOfReservedBuffers)
    {
        auto condition
            = [&availableBuffersLocked, numberOfReservedBuffers]() { return availableBuffersLocked->size() >= numberOfReservedBuffers; };
        bufferAvailableCondition.wait(availableBuffersLocked.as_lock(), condition);
    }

    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i)
    {
        buffers.emplace_back(availableBuffersLocked->back());
        availableBuffersLocked->pop_back();
    }

    auto localPool = std::make_shared<FixedSizeBufferPool>(shared_from_this(), buffers, numberOfReservedBuffers);
    localBufferPools.wlock()->emplace_back(localPool);
    return localPool;
}

std::optional<std::shared_ptr<AbstractBufferProvider>>
BufferManager::createFixedSizeBufferPool(size_t numberOfReservedBuffers, const std::stop_token& stopToken)
{
    std::deque<detail::MemorySegment*> buffers;
    auto availableBuffersLocked = availableBuffers.lock();
    NES_DEBUG(
        "Attempting to create fixed size buffer pool of {} Buffers. Currently available are {} buffers",
        numberOfReservedBuffers,
        availableBuffersLocked->size());

    if (availableBuffersLocked->size() < numberOfReservedBuffers)
    {
        auto condition
            = [&availableBuffersLocked, numberOfReservedBuffers]() { return availableBuffersLocked->size() >= numberOfReservedBuffers; };
        auto bufferAvailable = bufferAvailableCondition.wait(availableBuffersLocked.as_lock(), stopToken, condition);
        if (!bufferAvailable)
        {
            NES_WARNING("Could not allocate a fixed size buffer with {} buffers before cancellation.", numberOfReservedBuffers);
            return {};
        }
    }

    for (std::size_t i = 0; i < numberOfReservedBuffers; ++i)
    {
        buffers.emplace_back(availableBuffersLocked->back());
        availableBuffersLocked->pop_back();
    }

    auto localPool = std::make_shared<FixedSizeBufferPool>(shared_from_this(), buffers, numberOfReservedBuffers);
    localBufferPools.wlock()->emplace_back(localPool);
    return localPool;
}

}
