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
#include <cstdlib>
#include <execinfo.h>
#include <fstream>
#include <iostream>
#include <ranges>
#include <vector>
#include <limits>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <string>
#include <utility>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/ThreadLocalBufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <sanitizer/asan_interface.h>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

namespace
{
/// Reads a "Key:   <value> kB" line of /proc/self/status.
size_t readProcStatusKiB(const std::string_view key)
{
    std::ifstream status("/proc/self/status");
    for (std::string line; std::getline(status, line);)
    {
        if (line.starts_with(key))
        {
            return std::stoull(line.substr(key.size()));
        }
    }
    return 0;
}
}

uint32_t BufferManager::magazineSizeFor(const size_t numberOfBuffers)
{
    return static_cast<uint32_t>(std::clamp<size_t>(numberOfBuffers / 1024, 1, MAX_MAGAZINE_SIZE));
}

size_t BufferManager::numberOfMagazinesFor(const uint32_t magazineSize, const size_t numberOfBuffers)
{
    if (magazineSize == 0)
    {
        return 1;
    }
    /// Magazines for all buffers, plus two empty magazines for every cache.
    return ((numberOfBuffers + magazineSize - 1) / magazineSize) + (2 * (MAX_MAGAZINE_CACHES + 1)) + 2;
}

bool BufferManager::statsEnabled()
{
    static const bool enabled = std::getenv("NES_BUFFER_STATS") != nullptr;
    return enabled;
}

BufferManager::BufferManager(
    Private,
    const uint32_t bufferSize,
    const uint32_t numOfBuffers,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    const size_t unpooledMemoryLimitInBytes,
    const uint32_t alignment,
    const bool threadLocalCaches)
    : availableBuffers(threadLocalCaches ? 1 : numOfBuffers)
    , unpooledChunksManager(std::make_shared<UnpooledChunksManager>(memoryResource, unpooledMemoryLimitInBytes))
    , bufferSize(bufferSize)
    , numOfBuffers(numOfBuffers)
    , alignment(alignment)
    , memoryResource(std::move(memoryResource))
    , magazineSize(threadLocalCaches ? magazineSizeFor(numOfBuffers) : 0)
    , fullMagazines(numberOfMagazinesFor(magazineSize, numOfBuffers))
    , emptyMagazines(numberOfMagazinesFor(magazineSize, numOfBuffers))
{
    PRECONDITION(numOfBuffers > 0, "BufferManager requires at least one pooled buffer, but the configured budget yields {}", numOfBuffers);
    initialize();
    baselineRssKiB = readProcStatusKiB("VmRSS:");
}

std::shared_ptr<BufferManager> BufferManager::create(
    const size_t totalMemoryInBytes,
    const double unpooledMemoryFraction,
    const BufferAlignment alignment,
    const uint32_t bufferSize,
    const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
    const bool threadLocalCaches)
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
        Private{}, bufferSize, numOfBuffers, memoryResource, unpooledMemoryLimitInBytes, alignment.getRawValue(), threadLocalCaches);
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
        fullMagazines = decltype(fullMagazines)();
        emptyMagazines = decltype(emptyMagazines)();
        NES_DEBUG("Shutting down Buffer Manager completed");
        memoryResource->deallocate(basePointer, allocatedAreaSize, alignment);
        allocatedAreaSize = 0;

        if (statsEnabled())
        {
            const auto peakPooledBytes = peakPooledBuffersInUse.load() * bufferSize;
            const auto peakUnpooledBytes = unpooledChunksManager->getPeakAllocatedUnpooledBytes();
            const auto peakRssKiB = readProcStatusKiB("VmHWM:");
            std::cerr << fmt::format(
                "[BufferManager] peak memory: pooled {} MiB ({} of {} buffers), unpooled {} MiB, process rss above pool pre-allocation "
                "{} MiB (rss after pre-allocation {} MiB, peak rss {} MiB)\n",
                peakPooledBytes >> 20,
                peakPooledBuffersInUse.load(),
                numOfBuffers,
                peakUnpooledBytes >> 20,
                (peakRssKiB > baselineRssKiB ? peakRssKiB - baselineRssKiB : 0) >> 10,
                baselineRssKiB >> 10,
                peakRssKiB >> 10);
        }
        for (const auto* callSites : {&pooledCallSites, &unpooledCallSites})
        {
            std::vector<std::pair<std::vector<void*>, CallSite>> sites(callSites->begin(), callSites->end());
            std::ranges::sort(sites, [](const auto& a, const auto& b) { return a.second.count > b.second.count; });
            for (const auto& [frames, site] : std::views::take(sites, 12))
            {
                std::cerr << fmt::format(
                    "[{}CallSite] {} allocations, {} MiB total\n", callSites == &pooledCallSites ? "Pooled" : "Unpooled", site.count, site.bytes >> 20);
                char** symbols = backtrace_symbols(frames.data(), static_cast<int>(frames.size()));
                for (size_t i = 0; i < frames.size(); ++i)
                {
                    std::cerr << "    " << symbols[i] << "\n";
                }
                free(symbols); /// NOLINT
            }
        }

        /// Destroying the unpooled chunks
        unpooledChunksManager.reset();
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

    const std::span<uint8_t> allocatedMemory(basePointer, allocatedAreaSize);
    for (size_t i = 0; i < numOfBuffers; ++i)
    {
        const auto slot = allocatedMemory.subspan(i * offsetBetweenBuffers, offsetBetweenBuffers);
        const auto controlBlock = slot.subspan(redzoneSize, controlBlockSize);
        const auto payload = slot.subspan((2 * redzoneSize) + controlBlockSize, alignedBufferSize);
        allBuffers.emplace_back(
            payload.data(),
            bufferSize,
            [](detail::MemorySegment* segment, BufferRecycler* recycler) { recycler->recyclePooledBuffer(segment); },
            controlBlock.data());

        if (magazineSize == 0)
        {
            availableBuffers.write(&allBuffers.back());
        }
    }

    if (magazineSize > 0)
    {
        /// All buffers start in full magazines, and the remaining magazines start empty.
        const auto numberOfMagazines = numberOfMagazinesFor(magazineSize, numOfBuffers);
        magazineStorage = std::make_unique<Magazine[]>(numberOfMagazines);
        size_t nextMagazine = 0;
        for (size_t i = 0; i < allBuffers.size(); ++i)
        {
            auto* magazine = &magazineStorage[nextMagazine];
            magazine->items[magazine->count++] = &allBuffers[i];
            if (magazine->count == magazineSize)
            {
                ++nextMagazine;
                fullMagazines.write(magazine);
            }
        }
        if (magazineStorage[nextMagazine].count > 0)
        {
            fullMagazines.write(&magazineStorage[nextMagazine++]);
        }
        for (; nextMagazine < numberOfMagazines; ++nextMagazine)
        {
            emptyMagazines.write(&magazineStorage[nextMagazine]);
        }
    }

    /// All buffers start idle, so poison the whole arena in one shot: redzones, control blocks and data
    /// regions alike. The redzones stay poisoned for the arena's lifetime; prepare() unpoisons a control
    /// block on hand-out and recyclePooledBuffer() re-poisons it, and the data region is likewise unpoisoned
    /// on hand-out and re-poisoned on recycle. This must run after the construction loop so that the
    /// placement-new of each control block (and the debug marker fill above) writes to unpoisoned memory.
    ASAN_POISON_MEMORY_REGION(basePointer, allocatedAreaSize);
    NES_DEBUG("BufferManager configuration bufferSize={} numOfBuffers={}", this->bufferSize, this->numOfBuffers);
}

NES::detail::MemorySegment* BufferManager::takeFromCache(
    MagazineCache& cache, const bool block, const std::chrono::steady_clock::time_point deadline)
{
    if ((cache.loaded == nullptr || cache.loaded->count == 0) && cache.previous != nullptr && cache.previous->count > 0)
    {
        std::swap(cache.loaded, cache.previous);
    }
    if (cache.loaded == nullptr || cache.loaded->count == 0)
    {
        /// Both magazines are empty: exchange the loaded one for a full magazine from the depot.
        Magazine* full = nullptr;
        if (block ? fullMagazines.tryReadUntil(deadline, full) : fullMagazines.read(full))
        {
            if (cache.loaded != nullptr)
            {
                USED_IN_DEBUG const auto couldReturnEmpty = emptyMagazines.writeIfNotFull(cache.loaded);
                INVARIANT(couldReturnEmpty, "should always succeed");
            }
            cache.loaded = full;
        }
    }
    if (cache.loaded != nullptr && cache.loaded->count > 0)
    {
        return cache.loaded->items[--cache.loaded->count];
    }
    return nullptr;
}

void BufferManager::putIntoCache(MagazineCache& cache, NES::detail::MemorySegment* segment)
{
    if (cache.loaded == nullptr)
    {
        USED_IN_DEBUG const auto couldGetEmpty = emptyMagazines.read(cache.loaded);
        INVARIANT(couldGetEmpty, "there are empty magazines for every cache");
    }
    if (cache.loaded->count == magazineSize)
    {
        if (cache.previous != nullptr && cache.previous->count < magazineSize)
        {
            std::swap(cache.loaded, cache.previous);
        }
        else
        {
            /// Both magazines are full: hand the previous one to the depot and continue with an empty magazine.
            Magazine* empty = nullptr;
            USED_IN_DEBUG const auto couldGetEmpty = emptyMagazines.read(empty);
            INVARIANT(couldGetEmpty, "there are empty magazines for every cache");
            if (cache.previous != nullptr)
            {
                USED_IN_DEBUG const auto couldShipFull = fullMagazines.writeIfNotFull(cache.previous);
                INVARIANT(couldShipFull, "should always succeed");
            }
            cache.previous = cache.loaded;
            cache.loaded = empty;
        }
    }
    cache.loaded->items[cache.loaded->count++] = segment;
}

void BufferManager::releaseCache(MagazineCache& cache)
{
    if (isDestroyed)
    {
        return;
    }
    for (auto* magazine : {std::exchange(cache.loaded, nullptr), std::exchange(cache.previous, nullptr)})
    {
        if (magazine != nullptr)
        {
            USED_IN_DEBUG const auto couldReturn
                = magazine->count > 0 ? fullMagazines.writeIfNotFull(magazine) : emptyMagazines.writeIfNotFull(magazine);
            INVARIANT(couldReturn, "should always succeed");
        }
    }
}

NES::detail::MemorySegment* BufferManager::takeSegment(const bool block, const std::chrono::steady_clock::time_point deadline)
{
    if (magazineSize == 0)
    {
        NES::detail::MemorySegment* segment = nullptr;
        return (block ? availableBuffers.tryReadUntil(deadline, segment) : availableBuffers.read(segment)) ? segment : nullptr;
    }
    const std::scoped_lock lock(directCacheMutex);
    return takeFromCache(directCache, block, deadline);
}

void BufferManager::putSegment(NES::detail::MemorySegment* segment)
{
    if (magazineSize == 0)
    {
        USED_IN_DEBUG const auto couldRecycleBuffer = availableBuffers.writeIfNotFull(segment);
        INVARIANT(couldRecycleBuffer, "should always succeed");
        return;
    }
    const std::scoped_lock lock(directCacheMutex);
    putIntoCache(directCache, segment);
}

TupleBuffer BufferManager::makeBuffer(NES::detail::MemorySegment* segment)
{
#ifdef NDEBUG
    /// Release builds rely on the manager outliving its buffers, which saves the atomic reference count updates on the manager's shared state for every buffer.
    if (segment->controlBlock->prepareBorrowed(this))
#else
    /// Debug builds make the buffers keep the manager alive.
    if (segment->controlBlock->prepare(shared_from_this()))
#endif
    {
        /// The buffer is now exclusively owned by the caller: unpoison its data region so it can be used.
        ASAN_UNPOISON_MEMORY_REGION(segment->ptr, segment->size);
        if (statsEnabled())
        {
            updatePeakPooledBuffersInUse();
        }
        static const bool tracePooled = std::getenv("NES_TRACE_POOLED") != nullptr;
        if (tracePooled)
        {
            thread_local uint64_t sampleCounter = 0;
            if (++sampleCounter % 1024 == 0)
            {
                std::array<void*, 14> frames{};
                const auto depth = backtrace(frames.data(), frames.size());
                const std::scoped_lock lock(callSitesMutex);
                pooledCallSites[std::vector<void*>(frames.begin() + 2, frames.begin() + depth)].count += 1024;
            }
        }
        return TupleBuffer(segment->controlBlock.get(), segment->ptr, segment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

void BufferManager::updatePeakPooledBuffersInUse()
{
    const auto inUse = pooledBuffersInUse.fetch_add(1, std::memory_order_relaxed) + 1;
    auto peak = peakPooledBuffersInUse.load(std::memory_order_relaxed);
    while (inUse > peak && !peakPooledBuffersInUse.compare_exchange_weak(peak, inUse, std::memory_order_relaxed))
    {
    }
}

TupleBuffer BufferManager::getBufferBlocking()
{
    auto buffer = getBufferWithTimeout(GET_BUFFER_TIMEOUT);
    if (buffer.has_value())
    {
        return std::move(buffer.value());
    }
    /// Throw exception if no buffer was returned allocated after timeout.
    throw BufferAllocationFailure("Global buffer pool could not allocate buffer before timeout({})", GET_BUFFER_TIMEOUT);
}

std::optional<TupleBuffer> BufferManager::getBufferNoBlocking()
{
    auto* const segment = takeSegment(false, {});
    if (segment == nullptr)
    {
        return std::nullopt;
    }
    return makeBuffer(segment);
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    auto* const segment = takeSegment(true, std::chrono::steady_clock::now() + timeoutMs);
    if (segment == nullptr)
    {
        return std::nullopt;
    }
    return makeBuffer(segment);
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(const size_t bufferSize)
{
    static const bool traceUnpooled = std::getenv("NES_TRACE_UNPOOLED") != nullptr;
    if (traceUnpooled)
    {
        std::array<void*, 12> frames{};
        const auto depth = backtrace(frames.data(), frames.size());
        const std::scoped_lock lock(callSitesMutex);
        auto& callSite = unpooledCallSites[std::vector<void*>(frames.begin() + 1, frames.begin() + depth)];
        callSite.count += 1;
        callSite.bytes += bufferSize;
    }
    return unpooledChunksManager->getUnpooledBuffer(bufferSize, alignment, shared_from_this());
}

void BufferManager::recyclePooledBuffer(NES::detail::MemorySegment* segment)
{
    if (statsEnabled())
    {
        pooledBuffersInUse.fetch_sub(1, std::memory_order_relaxed);
    }
    INVARIANT(segment->isAvailable(), "Recycling buffer callback invoked on used memory segment");
    INVARIANT(
        segment->controlBlock->owningBufferRecycler == nullptr, "Buffer should not retain a reference to its parent while not in use");
    /// The buffer is back in the pool and must not be touched until handed out again: re-poison its data
    /// region and its control block. This is the control block's last read on the recycle path (the INVARIANTs
    /// above), and prepare() unpoisons it again on the next hand-out. Poisoning before the enqueue
    /// happens-before that unpoison via the availableBuffers queue, so there is no shadow-memory race.
    ASAN_POISON_MEMORY_REGION(segment->ptr, segment->size);
    ASAN_POISON_MEMORY_REGION(segment->controlBlock.get(), sizeof(detail::BufferControlBlock));
    if (magazineSize != 0)
    {
        /// The thread that returns a buffer caches it, whichever thread took it.
        if (auto* const provider = ThreadLocalBufferManager::current(); provider != nullptr && provider->manager.get() == this)
        {
            putIntoCache(provider->cache, segment);
            return;
        }
    }
    putSegment(segment);
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
    return unpooledChunksManager->getNumberOfUnpooledBuffers();
}

size_t BufferManager::getNumberOfAvailableBuffers() const
{
    /// If there are pending reads the queue may report negative values. This effectivly means its empty.
    if (magazineSize == 0)
    {
        return static_cast<size_t>(std::max(availableBuffers.size(), static_cast<ssize_t>(0)));
    }
    /// Every magazine is somewhere (a depot or a cache), and a magazine only holds buffers that are idle.
    size_t available = 0;
    for (size_t i = 0; i < numberOfMagazinesFor(magazineSize, numOfBuffers); ++i)
    {
        available += magazineStorage[i].count;
    }
    return available;
}

BufferManagerType BufferManager::getBufferManagerType() const
{
    return BufferManagerType::GLOBAL;
}

}
