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

#include <array>
#include <bit>
#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <utility>
#include <vector>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <folly/MPMCQueue.h>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES::detail
{

/// A pool of preallocated, fixed-size pooled buffers for a single size class.
///
/// This encapsulates exactly the machinery the BufferManager used to have for its single global
/// buffer size: a contiguous memory region whose buffers each carry a placement-new'd (Native)
/// BufferControlBlock, plus a lock-free folly::MPMCQueue of the currently available segments. The
/// multi-class BufferManager owns one FixedSizeClassPool per size class.
///
/// Memory is owned in `regions`: one region per (eager) preallocation or per (lazy) growth step.
/// Buffers are stored in a std::deque so their addresses stay stable as the pool grows -- the
/// availableBuffers queue and every handed-out TupleBuffer hold raw MemorySegment* into it.
class FixedSizeClassPool
{
public:
    FixedSizeClassPool(
        const uint32_t bufferSize,
        const size_t queueCapacity,
        const uint32_t alignment,
        std::shared_ptr<std::pmr::memory_resource> memoryResource,
        const bool elastic,
        const size_t growthChunkBuffers)
        : bufferSize(bufferSize)
        , alignment(alignment)
        , queueCapacity(queueCapacity)
        , elastic(elastic)
        , growthChunkBuffers(growthChunkBuffers)
        , memoryResource(std::move(memoryResource))
        , availableBuffers(queueCapacity)
    {
        PRECONDITION(queueCapacity > 0, "A size class pool needs a positive queue capacity");
        PRECONDITION(bufferSize > 0, "A size class pool needs a positive buffer size");
    }

    FixedSizeClassPool(const FixedSizeClassPool&) = delete;
    FixedSizeClassPool& operator=(const FixedSizeClassPool&) = delete;

    [[nodiscard]] uint32_t getBufferSize() const noexcept { return bufferSize; }

    /// Preallocate `count` buffers (clamped to the remaining queue capacity) as a single region.
    void addRegion(const size_t count)
    {
        const std::lock_guard lock(mutex);
        addRegionLocked(count);
    }

    /// Non-blocking pop of an available segment. If the pool is elastic and currently empty, it
    /// grows by one region and retries once. Returns nullptr if no buffer could be obtained.
    MemorySegment* tryPop()
    {
        MemorySegment* segment = nullptr;
        if (availableBuffers.read(segment))
        {
            return segment;
        }
        if (elastic)
        {
            const std::lock_guard lock(mutex);
            /// Only grow if still empty and below the capacity ceiling -- another thread may have grown already.
            if (allBuffers.size() < queueCapacity)
            {
                addRegionLocked(growthChunkBuffers);
            }
            if (availableBuffers.read(segment))
            {
                return segment;
            }
        }
        return nullptr;
    }

    /// Pop blocking until `deadline`. Elastic pools grow once up-front if empty so the wait can
    /// succeed immediately on the freshly added buffers.
    MemorySegment* popUntil(const std::chrono::steady_clock::time_point deadline)
    {
        if (elastic && numAvailable() == 0)
        {
            const std::lock_guard lock(mutex);
            if (allBuffers.size() < queueCapacity)
            {
                addRegionLocked(growthChunkBuffers);
            }
        }
        MemorySegment* segment = nullptr;
        if (availableBuffers.tryReadUntil(deadline, segment))
        {
            return segment;
        }
        return nullptr;
    }

    void recycle(MemorySegment* segment)
    {
        USED_IN_DEBUG const auto recycled = availableBuffers.writeIfNotFull(segment);
        INVARIANT(recycled, "recycling a pooled buffer should always succeed; the queue cannot be full");
    }

    [[nodiscard]] size_t numAvailable() const { return static_cast<size_t>(std::max(availableBuffers.size(), static_cast<ssize_t>(0))); }

    [[nodiscard]] size_t numTotal() const
    {
        const std::lock_guard lock(mutex);
        return allBuffers.size();
    }

    /// Direct access to the segments for leak inspection at shutdown. Not thread-safe; only call
    /// during destroy() when no buffers are being handed out.
    std::deque<MemorySegment>& segments() { return allBuffers; }

    /// Tear down: destroy all MemorySegments (which destroys the placement-new'd control blocks)
    /// and only then free the backing regions, matching the original BufferManager teardown order.
    void teardown()
    {
        allBuffers.clear();
        availableBuffers = folly::MPMCQueue<MemorySegment*>(1);
        for (auto& [base, size] : regions)
        {
            memoryResource->deallocate(base, size, alignment);
        }
        regions.clear();
    }

private:
    void addRegionLocked(size_t count)
    {
        if (count == 0)
        {
            return;
        }
        const size_t currentTotal = allBuffers.size();
        if (currentTotal + count > queueCapacity)
        {
            count = queueCapacity - currentTotal;
        }
        if (count == 0)
        {
            return;
        }

        const size_t controlBlockSize = alignBufferSize(sizeof(BufferControlBlock), alignment);
        const size_t alignedBufferSize = alignBufferSize(bufferSize, alignment);
        const size_t stride = alignBufferSize(controlBlockSize + alignedBufferSize, alignment);
        const size_t regionSize = stride * count;

        auto* base = static_cast<uint8_t*>(memoryResource->allocate(regionSize, alignment));
        INVARIANT(base, "memory allocation failed for a size class region");

#ifndef NDEBUG
        constexpr std::array marker{'N', 'E', 'B', 'U', 'S', 'T', 'R', 'M'};
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): freshly allocated raw storage aligned to >= alignof(uint64_t).
        std::fill_n(reinterpret_cast<uint64_t*>(base), regionSize / sizeof(uint64_t), std::bit_cast<uint64_t>(marker));
#endif

        regions.emplace_back(base, regionSize);
        uint8_t* ptr = base;
        for (size_t i = 0; i < count; ++i)
        {
            uint8_t* controlBlock = ptr;
            uint8_t* payload = ptr + controlBlockSize;
            allBuffers.emplace_back(
                payload,
                bufferSize,
                [](MemorySegment* segment, BufferRecycler* recycler) { recycler->recyclePooledBuffer(segment); },
                controlBlock);
            USED_IN_DEBUG const auto enqueued = availableBuffers.writeIfNotFull(&allBuffers.back());
            INVARIANT(enqueued, "queue unexpectedly full while initializing a size class region");
            ptr += stride;
        }
    }

    uint32_t bufferSize;
    uint32_t alignment;
    size_t queueCapacity;
    bool elastic;
    size_t growthChunkBuffers;
    std::shared_ptr<std::pmr::memory_resource> memoryResource;

    folly::MPMCQueue<MemorySegment*> availableBuffers;
    std::deque<MemorySegment> allBuffers;
    std::vector<std::pair<uint8_t*, size_t>> regions;
    mutable std::mutex mutex;
};

}
