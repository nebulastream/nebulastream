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
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
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
    std::optional<SizeClassConfig> sizeClasses)
    : unpooledChunksManager(std::make_shared<UnpooledChunksManager>(memoryResource))
    , bufferSize(bufferSize)
    , numOfBuffers(numOfBuffers)
    , memoryResource(std::move(memoryResource))
{
    initialize(sizeClasses);
}

std::shared_ptr<BufferManager> BufferManager::create(
    uint32_t bufferSize,
    uint32_t numOfBuffers,
    const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
    std::optional<SizeClassConfig> sizeClasses)
{
    return std::make_shared<BufferManager>(Private{}, bufferSize, numOfBuffers, memoryResource, std::move(sizeClasses));
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

void BufferManager::initialize(const std::optional<SizeClassConfig>& sizeClasses)
{
    /// Buffers are always aligned to a cache line; the alignment is no longer configurable.
    constexpr uint32_t withAlignment = DEFAULT_ALIGNMENT;
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
    if (sizeClasses.has_value())
    {
        const auto& classSpec = sizeClasses.value();
        PRECONDITION(
            std::has_single_bit(classSpec.minClassSize) && std::has_single_bit(classSpec.maxClassSize),
            "Size class bounds must be powers of two, got min={} max={}",
            classSpec.minClassSize,
            classSpec.maxClassSize);
        PRECONDITION(
            classSpec.minClassSize <= classSpec.maxClassSize,
            "minClassSize={} must be <= maxClassSize={}",
            classSpec.minClassSize,
            classSpec.maxClassSize);

        std::vector<size_t> classSizes;
        for (size_t classSize = classSpec.minClassSize; classSize <= classSpec.maxClassSize; classSize *= 2)
        {
            classSizes.push_back(classSize);
        }
        const size_t numClasses = classSizes.size();
        const size_t budget = (classSpec.totalBudgetBytes != 0) ? classSpec.totalBudgetBytes : (bufferSize * numOfBuffers);

        for (size_t classIndex = 0; classIndex < numClasses; ++classIndex)
        {
            const size_t classSize = classSizes[classIndex];
            PoolSpec spec{};
            switch (classSpec.policy)
            {
                case BufferProvisioningPolicy::TotalBudgetSplit: {
                    /// Bias the split so smaller classes get more buffers and larger classes fewer, while
                    /// keeping the total to `budget`. The weight runs linearly from 2.0 (smallest class) down
                    /// to 0.5 (largest) and is renormalized by its mean (1.25) so the shares still sum to
                    /// budget. Because buffers = bytes / classSize, this gives the smallest class 4x the
                    /// buffers of the largest at unchanged total memory. A single class takes the full budget.
                    constexpr double smallestWeight = 2.0;
                    constexpr double largestWeight = 0.5;
                    double share = 1.0;
                    if (numClasses > 1)
                    {
                        const double weight = smallestWeight
                            - ((smallestWeight - largestWeight) * static_cast<double>(classIndex) / static_cast<double>(numClasses - 1));
                        share = weight / ((smallestWeight + largestWeight) / 2.0); /// averages to 1 -> budget preserved
                    }
                    const auto perClassBytes = static_cast<size_t>((static_cast<double>(budget) / static_cast<double>(numClasses)) * share);
                    spec.initialCount = std::max<size_t>(1, perClassBytes / classSize);
                    spec.capacity = spec.initialCount;
                    spec.elastic = false;
                    spec.growthChunkBuffers = 0;
                    break;
                }
                case BufferProvisioningPolicy::EagerPerClass: {
                    spec.initialCount = std::max<size_t>(1, classSpec.buffersPerClass);
                    spec.capacity = spec.initialCount;
                    spec.elastic = false;
                    spec.growthChunkBuffers = 0;
                    break;
                }
                case BufferProvisioningPolicy::LazyElastic: {
                    spec.growthChunkBuffers = std::max<size_t>(1, classSpec.growthChunkBuffers);
                    spec.initialCount = classSpec.floorBuffersPerClass;
                    spec.capacity = std::max({classSpec.maxBuffersPerClass, spec.initialCount, spec.growthChunkBuffers});
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
    const double percentage = (100.0 * static_cast<double>(requiredMemorySpace)) / static_cast<double>(memorySizeInBytes);
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
    logPoolConfiguration();
}

void BufferManager::logPoolConfiguration() const
{
    /// Human-readable byte size, e.g. 4096 -> "4.0 KiB".
    const auto humanBytes = [](const size_t bytes)
    {
        static constexpr std::array<const char*, 4> units{"B", "KiB", "MiB", "GiB"};
        auto value = static_cast<double>(bytes);
        size_t unit = 0;
        while (value >= 1024.0 && unit + 1 < units.size())
        {
            value /= 1024.0;
            ++unit;
        }
        return fmt::format("{:.1f} {}", value, units[unit]);
    };

    std::string table = fmt::format("BufferManager pool configuration ({} size class(es)):\n", pools.size());
    table += "  +------------+----------+-----------+------------+\n";
    table += "  | size class |  buffers | available |   reserved |\n";
    table += "  +------------+----------+-----------+------------+\n";
    size_t totalReserved = 0;
    for (const auto& pool : pools)
    {
        const size_t classSize = pool->getBufferSize();
        const size_t total = pool->numTotal();
        const size_t reserved = classSize * total;
        totalReserved += reserved;
        table += fmt::format(
            "  | {:>10} | {:>8} | {:>9} | {:>10} |{}\n",
            humanBytes(classSize),
            total,
            pool->numAvailable(),
            humanBytes(reserved),
            classSize == bufferSize ? " <- default" : "");
    }
    table += "  +------------+----------+-----------+------------+\n";
    table += fmt::format("  total reserved: {}", humanBytes(totalReserved));
    NES_INFO("{}", table);
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
        NES_DEBUG("Fallback to UnpooledBuffer of size {} due to small maximum buffer size at \n{}", bufferSize, cpptrace::generate_trace(1).to_string());
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
    if (auto* segment = pools[index]->tryPop())
    {
        return wrapSegment(segment);
    }

    // NES_DEBUG("Fallback to UnpooledBuffer of size {} due to running out of buffers at \n{}", bufferSize, cpptrace::generate_trace(1).to_string());
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

size_t BufferManager::getMaxBufferSize() const
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
