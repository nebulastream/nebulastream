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
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <map>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <vector>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/UnpooledChunksManager.hpp>
#include <folly/MPMCQueue.h>

namespace NES
{

class ThreadLocalBufferManager;

/// Strong type for create()'s alignment argument to reduce potential swapped as both are u32 and also
/// readability-suspicious-call-argument on every call site
using BufferAlignment = NESStrongType<uint32_t, struct BufferAlignment_, 0, 0>;

/**
 * @brief The BufferManager is responsible for:
 * 1. Pooled Buffers: preallocated fixed-size buffers of memory that must be reference counted
 * 2. Unpooled Buffers: variable sized buffers that are allocated on-the-fly. They are also subject to reference
 * counting.
 *
 * The reference counting mechanism of the TupleBuffer is explained in TupleBuffer.hpp
 *
 * The BufferManager stores the pooled buffers as MemorySegment-s. When a component asks for a Pooled buffer,
 * then the BufferManager retrieves an available buffer (it blocks the calling thread, if no buffer is available).
 * It then hands out a TupleBuffer that is constructed through the pointer stored inside a MemorySegment.
 * This is necessary because the BufferManager must keep all buffers stored to ensure that when its
 * destructor is called, all buffers that it has ever created are deallocated. Note the BufferManager will check also
 * that no reference counter is non-zero and will throw a fatal exception, if a component hasnt returned every buffers.
 * This is necessary to avoid memory leaks.
 *
 * Unpooled buffers are either allocated on the spot or served via a previously allocated, unpooled buffer that has
 * been returned to the BufferManager by some component.
 *
 * Debug-only marker fill: in debug builds the pooled memory area is prefilled with the ASCII pattern "NEBUSTRM"
 * (repeated). This turns code paths that rely on freshly-allocated buffers being zeroed into visible bugs
 * (e.g. a sink that forgets to trim by the formatted byte count will emit "NEBUSTRM..." trailing garbage
 * instead of silently passing thanks to an implicit zero terminator). When debugging a buffer-handling
 * issue, grep dumps/logs for "NEBUSTRM" or 0x4D5254535542454E to spot reads of uninitialized buffer memory.
 */
class BufferManager final : public std::enable_shared_from_this<BufferManager>, public BufferRecycler, public AbstractBufferProvider
{
    friend class TupleBuffer;
    friend class NES::detail::MemorySegment;
    friend class ThreadLocalBufferManager;

    /// Hide the BufferManager constructor and only allow creation via BufferManager::create().
    /// Following: https://en.cppreference.com/w/cpp/memory/enable_shared_from_this
    struct Private
    {
        explicit Private() = default;
    };

public:
    explicit BufferManager(
        Private,
        uint32_t bufferSize,
        uint32_t numOfBuffers,
        std::shared_ptr<std::pmr::memory_resource> memoryResource,
        size_t unpooledMemoryLimitInBytes,
        uint32_t alignment,
        bool threadLocalCaches);

    /// Creates a new global buffer manager from a total memory budget. The pooled buffer count and the unpooled
    /// memory limit are derived: unpooledLimit = totalMemoryInBytes * unpooledMemoryFraction, the remaining
    /// pooled bytes are split into floor(pooledBytes / bufferSize) buffers.
    /// @param totalMemoryInBytes total memory budget shared by the pooled and unpooled pools
    /// @param unpooledMemoryFraction share of the total budget reserved for unpooled buffers, in [0.0, 1.0]
    /// @param alignment byte alignment of every buffer; must be a power of two <= page size (a cache line is 64 bytes)
    /// @param bufferSize the size of each pooled buffer in bytes
    /// @param memoryResource resource for allocating and deallocating memory
    /// @param threadLocalCaches if true, the pooled buffers are kept in magazines of 64 buffers, which threads can cache with a
    /// ThreadLocalBufferManager so that they only touch the shared state once per magazine. Buffers cached by a thread are invisible to
    /// the others, so this is meant for large pools. Without it, the manager is a plain queue of buffers.
    static std::shared_ptr<BufferManager> create(
        size_t totalMemoryInBytes,
        double unpooledMemoryFraction,
        BufferAlignment alignment,
        uint32_t bufferSize,
        const std::shared_ptr<std::pmr::memory_resource>& memoryResource,
        bool threadLocalCaches = false);

    [[nodiscard]] bool usesThreadLocalCaches() const { return magazineSize != 0; }

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager() override;

    BufferManagerType getBufferManagerType() const override;

private:
    /**
     * @brief Configure the BufferManager to use numOfBuffers buffers of size bufferSize bytes.
     * This is a one shot call. A second invocation of this call will fail
     */
    void initialize();

public:
    /// This blocks until a buffer is available.
    TupleBuffer getBufferBlocking() override;

    /// invalid optional if there is no buffer.
    std::optional<TupleBuffer> getBufferNoBlocking() override;

    /**
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
     * timeoutMs.
     * @param timeoutMs the amount of time to wait for a new buffer to be retuned
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeoutMs) override;

    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override;


    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    size_t getNumberOfAvailableBuffers() const;

    /// Explicitly shuts down the buffer manager: checks for leaked buffers (fires INVARIANT on leaks),
    /// deallocates all memory, and marks the manager as destroyed. The destructor calls this automatically
    /// if it has not already been called.
    void destroy();

    /**
     * @brief Recycle a pooled buffer by making it available to others
     * @param buffer
     */
    void recyclePooledBuffer(NES::detail::MemorySegment* segment) override;

    /**
    * @brief Recycle an unpooled buffer by making it available to others
    * @param buffer
    */
    void recycleUnpooledBuffer(NES::detail::MemorySegment* segment, const AllocationThreadInfo&) override;

private:
    /// A magazine is a batch of idle pooled buffers that moves between the depots and a cache as a whole.
    static constexpr uint32_t MAX_MAGAZINE_SIZE = 64;
    static constexpr size_t MAX_MAGAZINE_CACHES = 1024;
    struct Magazine
    {
        uint32_t count = 0;
        std::array<NES::detail::MemorySegment*, MAX_MAGAZINE_SIZE> items{};
    };
    /// A cache holds a loaded and a previous magazine, which gives it hysteresis between taking and returning buffers. It is only
    /// used by one thread at a time: the calling thread of a ThreadLocalBufferManager, or the callers of the manager itself under
    /// directCacheMutex.
    struct MagazineCache
    {
        Magazine* loaded = nullptr;
        Magazine* previous = nullptr;
    };
    static uint32_t magazineSizeFor(size_t numberOfBuffers);
    static size_t numberOfMagazinesFor(uint32_t magazineSize, size_t numberOfBuffers);

    /// Takes a buffer through the cache, returns nullptr if there is none (immediately if block is false, after the deadline otherwise).
    NES::detail::MemorySegment* takeFromCache(MagazineCache& cache, bool block, std::chrono::steady_clock::time_point deadline);
    void putIntoCache(MagazineCache& cache, NES::detail::MemorySegment* segment);
    /// Hands the magazines of a cache back to the depots.
    void releaseCache(MagazineCache& cache);
    /// Takes and returns buffers through the queue or the direct cache, for callers without a ThreadLocalBufferManager.
    NES::detail::MemorySegment* takeSegment(bool block, std::chrono::steady_clock::time_point deadline);
    void putSegment(NES::detail::MemorySegment* segment);
    TupleBuffer makeBuffer(NES::detail::MemorySegment* segment);

    std::vector<NES::detail::MemorySegment> allBuffers;

    /// The idle buffers of a manager without thread local caches.
    folly::MPMCQueue<NES::detail::MemorySegment*> availableBuffers;

    std::shared_ptr<NES::UnpooledChunksManager> unpooledChunksManager;

    size_t bufferSize;
    size_t numOfBuffers;
    uint32_t alignment;

    uint8_t* basePointer{nullptr};
    size_t allocatedAreaSize;

    std::shared_ptr<std::pmr::memory_resource> memoryResource;
    std::atomic<bool> isDestroyed{false};

    /// 0 if the manager has no thread local caches.
    uint32_t magazineSize;
    std::unique_ptr<Magazine[]> magazineStorage;
    /// The idle buffers of a manager with thread local caches are all in magazines: in these depots or in a cache.
    folly::MPMCQueue<Magazine*> fullMagazines;
    folly::MPMCQueue<Magazine*> emptyMagazines;
    std::atomic<size_t> numberOfCaches{0};
    std::mutex directCacheMutex;
    MagazineCache directCache;

    /// Diagnostics, enabled with NES_BUFFER_STATS: the highest number of simultaneously handed out pooled buffers, to tune the
    /// pooled/unpooled memory split, and the process memory on top of the pre-allocated pool.
    static bool statsEnabled();
    std::atomic<size_t> pooledBuffersInUse{0};
    std::atomic<size_t> peakPooledBuffersInUse{0};
    void updatePeakPooledBuffersInUse();
    /// Resident set size of the process right after the pool was pre-allocated.
    size_t baselineRssKiB = 0;

    /// Diagnostics: with NES_TRACE_UNPOOLED / NES_TRACE_POOLED set, counts the (sampled) allocations per call stack.
    struct CallSite
    {
        size_t count = 0;
        size_t bytes = 0;
    };
    std::mutex callSitesMutex;
    std::map<std::vector<void*>, CallSite> unpooledCallSites;
    std::map<std::vector<void*>, CallSite> pooledCallSites;
};


}
