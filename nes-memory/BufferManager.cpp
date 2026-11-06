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
#include <ranges>
#include <source_location>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/BufferRequestStatistics.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <sanitizer/asan_interface.h>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{

BufferManager::BufferManager(
    Private,
    const uint32_t bufferSize,
    const uint32_t numOfBuffers,
    std::shared_ptr<std::pmr::memory_resource> memoryResource,
    const size_t unpooledMemoryLimitInBytes,
    const uint32_t alignment)
    : availableBuffers(numOfBuffers)
    , unpooledChunksManager(std::make_shared<UnpooledChunksManager>(memoryResource, unpooledMemoryLimitInBytes, memoryInUse))
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
    const std::shared_ptr<std::pmr::memory_resource>& memoryResource)
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
        Private{}, bufferSize, numOfBuffers, memoryResource, unpooledMemoryLimitInBytes, alignment.getRawValue());
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
        printRequestStatistics();
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
    NES_DEBUG(
        "NES memory allocation requires {} out of {} (so {}%) available bytes",
        requiredMemorySpace,
        memorySizeInBytes,
        (100.0 * requiredMemorySpace) / memorySizeInBytes);

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

        availableBuffers.write(&allBuffers.back());
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
    const bool served = availableBuffers.read(memSegment);
    requestStatistics.recordPooledRequest(served);
    if (!served)
    {
        return std::nullopt;
    }
    memoryInUse->addPooled(bufferSize);
    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        /// The buffer is now exclusively owned by the caller: unpoison its data region so it can be used.
        ASAN_UNPOISON_MEMORY_REGION(memSegment->ptr, memSegment->size);
        /// Zeroed so that recyclePooledBuffer can find how far the buffer was written.
        std::memset(memSegment->ptr, 0, memSegment->size);
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getBufferWithTimeout(const std::chrono::milliseconds timeoutMs)
{
    detail::MemorySegment* memSegment = nullptr;
    const auto deadline = std::chrono::steady_clock::now() + timeoutMs;
    const bool served = availableBuffers.tryReadUntil(deadline, memSegment);
    requestStatistics.recordPooledRequest(served);
    if (!served)
    {
        return std::nullopt;
    }
    memoryInUse->addPooled(bufferSize);
    if (memSegment->controlBlock->prepare(shared_from_this()))
    {
        /// The buffer is now exclusively owned by the caller: unpoison its data region so it can be used.
        ASAN_UNPOISON_MEMORY_REGION(memSegment->ptr, memSegment->size);
        /// Zeroed so that recyclePooledBuffer can find how far the buffer was written.
        std::memset(memSegment->ptr, 0, memSegment->size);
        return TupleBuffer(memSegment->controlBlock.get(), memSegment->ptr, memSegment->size);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

std::optional<TupleBuffer> BufferManager::getUnpooledBuffer(const size_t bufferSize, const std::source_location caller)
{
    auto buffer = unpooledChunksManager->getUnpooledBuffer(
        bufferSize, alignment, shared_from_this(), requestStatistics.liveUnpooledBuffers(caller));
    requestStatistics.recordUnpooledRequest(bufferSize, buffer.has_value(), caller);
    return buffer;
}

BufferRequestStatistics::Snapshot BufferManager::getRequestStatistics() const
{
    return requestStatistics.snapshot();
}

namespace
{
std::string withThousandsSeparators(const uint64_t value)
{
    auto digits = std::to_string(value);
    for (auto pos = static_cast<int64_t>(digits.size()) - 3; pos > 0; pos -= 3)
    {
        digits.insert(static_cast<size_t>(pos), ",");
    }
    return digits;
}
}

void BufferManager::printRequestStatistics() const
{
    const auto stats = requestStatistics.snapshot();
    uint64_t unpooledBytes = 0;
    for (const auto& [size, count] : stats.unpooledRequestedSizeCounts)
    {
        unpooledBytes += size * count;
    }
    auto out = fmt::format(
        "\n[BufferRequestStatistics] {}\n"
        "  - pool: {} buffers of {} B ({} B)\n"
        "  - pooled buffers requested:   {} (failed: {}), {} B\n"
        "  - unpooled buffers requested: {} (failed: {}), {} B\n",
        name,
        withThousandsSeparators(numOfBuffers),
        withThousandsSeparators(bufferSize),
        withThousandsSeparators(numOfBuffers * bufferSize),
        withThousandsSeparators(stats.pooledRequests),
        withThousandsSeparators(stats.pooledFailed),
        withThousandsSeparators(stats.pooledRequests * bufferSize),
        withThousandsSeparators(stats.unpooledRequests),
        withThousandsSeparators(stats.unpooledFailed),
        withThousandsSeparators(unpooledBytes));
    if (const auto& sizes = stats.unpooledRequestedSize)
    {
        out += fmt::format(
            "  - unpooled requested size:\n"
            "      - min:    {} B\n"
            "      - max:    {} B\n"
            "      - avg:    {:.1f} B\n"
            "      - median: {:.1f} B\n",
            withThousandsSeparators(sizes->min),
            withThousandsSeparators(sizes->max),
            sizes->avg,
            sizes->median);
        out += "  - unpooled requested size breakdown:\n";
        for (const auto& [size, count] : stats.unpooledRequestedSizeCounts)
        {
            out += fmt::format(
                "      - {:>10} B: {:>12} times ({:5.2f}%), {:>17} B ({:5.2f}%)\n",
                withThousandsSeparators(size),
                withThousandsSeparators(count),
                100.0 * static_cast<double>(count) / static_cast<double>(stats.unpooledRequests),
                withThousandsSeparators(size * count),
                100.0 * static_cast<double>(size * count) / static_cast<double>(unpooledBytes));
        }
        out += "  - unpooled requests per call site:\n";
        for (const auto& callSite : stats.unpooledCallSites)
        {
            uint64_t callSiteBytes = 0;
            std::vector<std::pair<size_t, uint64_t>> sizesByCount(callSite.requestedSizeCounts.begin(), callSite.requestedSizeCounts.end());
            for (const auto& [size, count] : sizesByCount)
            {
                callSiteBytes += size * count;
            }
            std::ranges::sort(sizesByCount, std::ranges::greater{}, &std::pair<size_t, uint64_t>::second);
            out += fmt::format(
                "      - {}\n"
                "          - {} times ({:5.2f}%, failed: {}), {} B ({:5.2f}%)\n"
                "          - peak live: {} buffers, {} B\n"
                "          - sizes:",
                callSite.location,
                withThousandsSeparators(callSite.requests),
                100.0 * static_cast<double>(callSite.requests) / static_cast<double>(stats.unpooledRequests),
                withThousandsSeparators(callSite.failed),
                withThousandsSeparators(callSiteBytes),
                100.0 * static_cast<double>(callSiteBytes) / static_cast<double>(unpooledBytes),
                withThousandsSeparators(callSite.peakLiveBuffers),
                withThousandsSeparators(callSite.peakLiveBytes));
            /// Varsized call sites request many distinct sizes, so only the most frequent ones are printed.
            constexpr size_t printedSizes = 5;
            for (const auto& [size, count] : sizesByCount | std::views::take(printedSizes))
            {
                out += fmt::format(" {} B x {},", withThousandsSeparators(size), withThousandsSeparators(count));
            }
            out.pop_back();
            if (sizesByCount.size() > printedSizes)
            {
                out += fmt::format(" ... {} more sizes", sizesByCount.size() - printedSizes);
            }
            out += "\n";
        }
    }
    const auto peaks = memoryInUse->peaks();
    out += fmt::format(
        "  - peak in use:\n"
        "      - pooled:        {} buffers, {} B\n"
        "      - unpooled:      {} B in chunks, {} B in live buffers\n"
        "      - combined:      {} B (pooled + unpooled chunks)\n",
        withThousandsSeparators(peaks.pooledBytes / bufferSize),
        withThousandsSeparators(peaks.pooledBytes),
        withThousandsSeparators(peaks.unpooledChunkBytes),
        withThousandsSeparators(peaks.unpooledLiveBytes),
        withThousandsSeparators(peaks.combinedBytes));
    if (const auto& fill = stats.pooledWrittenBytesCounts; not fill.empty())
    {
        uint64_t buffers = 0;
        uint64_t writtenBytes = 0;
        for (const auto& [bytes, count] : fill)
        {
            buffers += count;
            writtenBytes += bytes * count;
        }
        const auto capacityBytes = buffers * bufferSize;
        auto fillPercent = [&](const size_t bytes) { return 100.0 * static_cast<double>(bytes) / static_cast<double>(bufferSize); };
        /// Fill of the buffer at the given rank, counted from the emptiest buffer.
        auto fillAtRank = [&](const uint64_t rank)
        {
            uint64_t seen = 0;
            for (const auto& [bytes, count] : fill)
            {
                seen += count;
                if (seen > rank)
                {
                    return fillPercent(bytes);
                }
            }
            return fillPercent(fill.rbegin()->first);
        };
        /// Buffers per fill percentage, rounded down; 0% only for untouched buffers and 100% only for full ones.
        std::array<uint64_t, 101> byPercent{};
        for (const auto& [bytes, count] : fill)
        {
            byPercent[bytes == 0 ? 0 : std::max<size_t>(1, bytes * 100 / bufferSize)] += count;
        }
        out += fmt::format(
            "  - pooled buffer fill (bytes up to the last non-zero byte when recycled):\n"
            "      - buffers:  {} recycled, {} B capacity, {} B written ({:.1f}%), {} B wasted ({:.1f}%)\n"
            "      - fill per buffer: min {:.1f}%, p10 {:.1f}%, median {:.1f}%, avg {:.1f}%, p90 {:.1f}%, max {:.1f}%\n",
            withThousandsSeparators(buffers),
            withThousandsSeparators(capacityBytes),
            withThousandsSeparators(writtenBytes),
            100.0 * static_cast<double>(writtenBytes) / static_cast<double>(capacityBytes),
            withThousandsSeparators(capacityBytes - writtenBytes),
            100.0 * static_cast<double>(capacityBytes - writtenBytes) / static_cast<double>(capacityBytes),
            fillPercent(fill.begin()->first),
            fillAtRank((buffers - 1) / 10),
            fillAtRank((buffers - 1) / 2),
            100.0 * static_cast<double>(writtenBytes) / static_cast<double>(capacityBytes),
            fillAtRank((buffers - 1) * 9 / 10),
            fillPercent(fill.rbegin()->first));
        /// [0%, 25%), [25%, 50%), [50%, 75%), [75%, 100%], full buffers in the last group.
        std::array<uint64_t, 4> byQuarter{};
        for (size_t percent = 0; percent < byPercent.size(); ++percent)
        {
            byQuarter[std::min<size_t>(percent / 25, 3)] += byPercent[percent];
        }
        out += "      - buffers per fill quarter:\n";
        for (size_t quarter = 0; quarter < byQuarter.size(); ++quarter)
        {
            out += fmt::format(
                "      - [{:>2}%, {:>3}%{}: {:>12} buffers ({:5.2f}%)\n",
                quarter * 25,
                (quarter + 1) * 25,
                quarter == 3 ? "]" : ")",
                withThousandsSeparators(byQuarter[quarter]),
                100.0 * static_cast<double>(byQuarter[quarter]) / static_cast<double>(buffers));
        }
        out += "      - buffers per fill percentage:\n";
        for (size_t percent = 0; percent < byPercent.size(); ++percent)
        {
            if (byPercent[percent] != 0)
            {
                out += fmt::format(
                    "      - {:>3}%: {:>12} buffers ({:5.2f}%)\n",
                    percent,
                    withThousandsSeparators(byPercent[percent]),
                    100.0 * static_cast<double>(byPercent[percent]) / static_cast<double>(buffers));
            }
        }
    }
    /// One write per block so blocks of concurrently destroyed BufferManagers do not interleave.
    fmt::print(stderr, "{}", out);
}

void BufferManager::setName(std::string name)
{
    this->name = std::move(name);
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
    const auto data = std::span{segment->ptr, segment->size} | std::views::reverse;
    const auto lastWritten = std::ranges::find_if(data, [](const uint8_t byte) { return byte != 0; });
    requestStatistics.recordPooledFill(static_cast<size_t>(std::ranges::distance(lastWritten, data.end())));
    ASAN_POISON_MEMORY_REGION(segment->ptr, segment->size);
    ASAN_POISON_MEMORY_REGION(segment->controlBlock.get(), sizeof(detail::BufferControlBlock));
    memoryInUse->removePooled(bufferSize);
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
