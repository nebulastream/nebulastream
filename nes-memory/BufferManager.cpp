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
#include <bit>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <utility>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FixedSizeClassPool.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

BufferManager::BufferManager(
    Private,
    const uint32_t bufferSize,
    const uint32_t numOfBuffers,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    const uint32_t withAlignment,
    std::optional<SizeClassConfig> sizeClasses)
    : unpooledChunksManager(std::make_shared<UnpooledChunksManager>(memoryResource))
    , bufferSize(bufferSize)
    , numOfBuffers(numOfBuffers)
    , memoryResource(std::move(memoryResource))
{
    ((void)withAlignment);
    initialize(DEFAULT_ALIGNMENT, sizeClasses);
}

std::shared_ptr<BufferManager> BufferManager::create(
    uint32_t bufferSize,
    uint32_t numOfBuffers,
    const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
    uint32_t withAlignment,
    std::optional<SizeClassConfig> sizeClasses)
{
    return std::make_shared<BufferManager>(Private{}, bufferSize, numOfBuffers, memoryResource, withAlignment, std::move(sizeClasses));
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
        size_t totalBuffers = 0;
        size_t availableBuffers = 0;
        for (const auto& pool : pools)
        {
            totalBuffers += pool->numTotal();
            availableBuffers += pool->numAvailable();
        }
        if (totalBuffers != availableBuffers)
        {
            NES_ERROR("[BufferManager] total buffers {} :: available buffers {}", totalBuffers, availableBuffers);
            success = false;
        }
        for (const auto& pool : pools)
        {
            for (auto& buffer : pool->segments())
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
        }
        INVARIANT(
            success,
            "Requested buffer manager shutdown but a buffer is still used allBuffers={} available={}",
            totalBuffers,
            availableBuffers);

        /// Destroy the MemorySegments (and their placed control blocks) before freeing the backing regions.
        for (const auto& pool : pools)
        {
            pool->teardown();
        }
        pools.clear();
        NES_DEBUG("Shutting down Buffer Manager completed");

        /// Destroying the unpooled chunks
        unpooledChunksManager.reset();
    }
}

namespace
{
/// Per-pool provisioning derived from the SizeClassConfig before any memory is allocated.
struct PoolSpec
{
    size_t initialCount;
    size_t capacity;
    bool elastic;
    size_t growthChunkBuffers;
};
}

void BufferManager::initialize(uint32_t withAlignment, const std::optional<SizeClassConfig>& sizeClasses)
{
    const size_t pages = sysconf(_SC_PHYS_PAGES);
    const size_t pageSize = sysconf(_SC_PAGE_SIZE);
    const auto memorySizeInBytes = pages * pageSize;

    if (withAlignment > 0)
    {
        PRECONDITION(
            !(withAlignment & (withAlignment - 1)), "NES tries to align memory but alignment={} is not a pow of two", withAlignment);
    }
    PRECONDITION(withAlignment <= pageSize, "NES tries to align memory but alignment is invalid: {} <= {}", withAlignment, pageSize);
    PRECONDITION(
        alignof(detail::BufferControlBlock) <= withAlignment,
        "Requested alignment is too small, must be at least {}",
        alignof(detail::BufferControlBlock));
    this->alignment = withAlignment;

    /// std::map keeps the size classes sorted ascending and deduplicates a configured class that
    /// coincides with the default buffer size (the default class' provisioning wins for that size).
    std::map<size_t, PoolSpec> specs;
    specs[bufferSize] = PoolSpec{.initialCount = numOfBuffers, .capacity = numOfBuffers, .elastic = false, .growthChunkBuffers = 0};

    if (sizeClasses.has_value())
    {
        const auto& sc = sizeClasses.value();
        PRECONDITION(
            std::has_single_bit(sc.minClassSize) && std::has_single_bit(sc.maxClassSize),
            "Size class bounds must be powers of two, got min={} max={}",
            sc.minClassSize,
            sc.maxClassSize);
        PRECONDITION(sc.minClassSize <= sc.maxClassSize, "minClassSize={} must be <= maxClassSize={}", sc.minClassSize, sc.maxClassSize);

        std::vector<size_t> classSizes;
        for (size_t classSize = sc.minClassSize; classSize <= sc.maxClassSize; classSize *= 2)
        {
            classSizes.push_back(classSize);
        }
        const size_t numClasses = classSizes.size();
        const size_t budget = (sc.totalBudgetBytes != 0) ? sc.totalBudgetBytes : (static_cast<size_t>(bufferSize) * numOfBuffers);

        for (const size_t classSize : classSizes)
        {
            if (specs.contains(classSize))
            {
                continue; /// already provided by the default class
            }
            PoolSpec spec{};
            switch (sc.policy)
            {
                case BufferProvisioningPolicy::TotalBudgetSplit: {
                    const size_t perClassBytes = budget / numClasses;
                    spec.initialCount = std::max<size_t>(1, perClassBytes / classSize);
                    spec.capacity = spec.initialCount;
                    spec.elastic = false;
                    spec.growthChunkBuffers = 0;
                    break;
                }
                case BufferProvisioningPolicy::EagerPerClass: {
                    spec.initialCount = std::max<size_t>(1, sc.buffersPerClass);
                    spec.capacity = spec.initialCount;
                    spec.elastic = false;
                    spec.growthChunkBuffers = 0;
                    break;
                }
                case BufferProvisioningPolicy::LazyElastic: {
                    spec.growthChunkBuffers = std::max<size_t>(1, sc.growthChunkBuffers);
                    spec.initialCount = sc.floorBuffersPerClass;
                    spec.capacity = std::max({sc.maxBuffersPerClass, spec.initialCount, spec.growthChunkBuffers});
                    spec.elastic = true;
                    break;
                }
            }
            specs[classSize] = spec;
        }
    }

    /// Verify the eager preallocation fits into physical memory.
    const size_t controlBlockSize = alignBufferSize(sizeof(detail::BufferControlBlock), withAlignment);
    uint64_t requiredMemorySpace = 0;
    for (const auto& [classSize, spec] : specs)
    {
        const size_t alignedBufferSize = alignBufferSize(classSize, withAlignment);
        const size_t stride = alignBufferSize(controlBlockSize + alignedBufferSize, withAlignment);
        requiredMemorySpace += stride * spec.initialCount;
    }
    const double percentage = (100.0 * requiredMemorySpace) / memorySizeInBytes;
    NES_DEBUG("NES memory allocation requires {} out of {} (so {}%) available bytes", requiredMemorySpace, memorySizeInBytes, percentage);
    INVARIANT(
        requiredMemorySpace < memorySizeInBytes,
        "NES tries to allocate more memory than physically available requested={} available={}",
        requiredMemorySpace,
        memorySizeInBytes);

    /// Build the pools (std::map iterates ascending by class size).
    pools.reserve(specs.size());
    size_t index = 0;
    for (const auto& [classSize, spec] : specs)
    {
        auto pool = std::make_unique<detail::FixedSizeClassPool>(
            static_cast<uint32_t>(classSize),
            std::max<size_t>(1, spec.capacity),
            withAlignment,
            memoryResource,
            spec.elastic,
            spec.growthChunkBuffers);
        if (spec.initialCount > 0)
        {
            pool->addRegion(spec.initialCount);
        }
        if (classSize == bufferSize)
        {
            defaultPoolIndex = index;
        }
        pools.push_back(std::move(pool));
        ++index;
    }
    NES_DEBUG("BufferManager configured with {} size class(es), default class bufferSize={}", pools.size(), bufferSize);
}

size_t BufferManager::classIndexForSize(const size_t size) const noexcept
{
    for (size_t i = 0; i < pools.size(); ++i)
    {
        if (pools[i]->getBufferSize() >= size)
        {
            return i;
        }
    }
    return pools.size();
}

TupleBuffer BufferManager::wrapSegment(detail::MemorySegment* segment)
{
    if (segment->controlBlock->prepare(shared_from_this()))
    {
        return TupleBuffer(segment->controlBlock.get(), segment->ptr, segment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

TupleBuffer BufferManager::getBufferBlocking()
{
    if (auto* segment = pools[defaultPoolIndex]->popUntil(std::chrono::steady_clock::now() + GET_BUFFER_TIMEOUT))
    {
        return wrapSegment(segment);
    }
    /// Throw exception if no buffer was returned allocated after timeout.
    throw BufferAllocationFailure("Global buffer pool could not allocate buffer before timeout({})", GET_BUFFER_TIMEOUT);
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking()
{
    if (auto* segment = pools[defaultPoolIndex]->tryPop())
    {
        return wrapSegment(segment);
    }
    return std::nullopt;
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    if (auto* segment = pools[defaultPoolIndex]->popUntil(std::chrono::steady_clock::now() + timeoutMs))
    {
        return wrapSegment(segment);
    }
    return std::nullopt;
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking(const size_t size)
{
    const size_t index = classIndexForSize(size);
    if (index >= pools.size())
    {
        /// Larger than any size class -> serve from the unpooled path.
        return getUnpooledBuffer(size);
    }
    /// Best-fit class first, then promote to larger classes if it is momentarily exhausted.
    for (size_t i = index; i < pools.size(); ++i)
    {
        if (auto* segment = pools[i]->tryPop())
        {
            return wrapSegment(segment);
        }
    }
    return std::nullopt;
}

TupleBuffer BufferManager::getBuffer(const size_t size)
{
    const size_t index = classIndexForSize(size);
    if (index >= pools.size())
    {
        if (auto unpooled = getUnpooledBuffer(size))
        {
            return std::move(unpooled.value());
        }
        throw BufferAllocationFailure("Buffer manager could not allocate an unpooled buffer of size {}", size);
    }

    /// Fast path: best-fit or any larger class without blocking.
    if (auto nonBlocking = getBufferNoBlocking(size))
    {
        return std::move(nonBlocking.value());
    }

    /// Slow path: block on the best-fit class until the timeout elapses.
    if (auto* segment = pools[index]->popUntil(std::chrono::steady_clock::now() + GET_BUFFER_TIMEOUT))
    {
        return wrapSegment(segment);
    }

    /// Last resort: fall back to the unpooled path rather than failing.
    if (auto unpooled = getUnpooledBuffer(size))
    {
        return std::move(unpooled.value());
    }
    throw BufferAllocationFailure("Buffer manager could not allocate a buffer of size {} before timeout({})", size, GET_BUFFER_TIMEOUT);
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
    const uint32_t size = segment->size;
    for (const auto& pool : pools)
    {
        if (pool->getBufferSize() == size)
        {
            pool->recycle(segment);
            return;
        }
    }
    INVARIANT(false, "Recycled a pooled buffer whose size {} matches no size class", size);
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
    size_t total = 0;
    for (const auto& pool : pools)
    {
        total += pool->numTotal();
    }
    return total;
}

size_t BufferManager::getNumOfUnpooledBuffers() const
{
    return unpooledChunksManager->getNumberOfUnpooledBuffers();
}

size_t BufferManager::getNumberOfAvailableBuffers() const
{
    size_t available = 0;
    for (const auto& pool : pools)
    {
        available += pool->numAvailable();
    }
    return available;
}

BufferManagerType BufferManager::getBufferManagerType() const
{
    return BufferManagerType::GLOBAL;
}

}
