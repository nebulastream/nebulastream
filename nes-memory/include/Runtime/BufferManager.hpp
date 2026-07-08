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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <optional>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/UnpooledChunksManager.hpp>

namespace NES::detail
{
class FixedSizeClassPool;
}

namespace NES
{

/// How the buffers of the additional (power-of-two) size classes are provisioned. The default
/// class (of exactly the configured bufferSize) is always eagerly preallocated for backward
/// compatibility; this policy only governs the extra size classes.
enum class BufferProvisioningPolicy : uint8_t
{
    /// Distribute one overall byte budget equally across all size classes (counts derived per class).
    TotalBudgetSplit,
    /// Eagerly preallocate a fixed number of buffers per size class.
    EagerPerClass,
    /// Preallocate a small floor per class and grow it by a region on demand, up to a ceiling.
    LazyElastic
};

/// Opt-in configuration enabling additional power-of-two size classes alongside the default buffer
/// size. Classes are minClassSize, 2*minClassSize, ... up to maxClassSize (both powers of two).
struct SizeClassConfig
{
    size_t minClassSize;
    size_t maxClassSize;
    BufferProvisioningPolicy policy = BufferProvisioningPolicy::TotalBudgetSplit;
    /// TotalBudgetSplit: total bytes distributed (equal share) across the size classes.
    size_t totalBudgetBytes = 0;
    /// EagerPerClass: buffers preallocated per size class.
    size_t buffersPerClass = 0;
    /// LazyElastic: floor preallocated per class, region growth step, and per-class ceiling.
    static constexpr size_t DEFAULT_GROWTH_CHUNK_BUFFERS = 64;
    static constexpr size_t DEFAULT_MAX_BUFFERS_PER_CLASS = 4096;
    size_t floorBuffersPerClass = 0;
    size_t growthChunkBuffers = DEFAULT_GROWTH_CHUNK_BUFFERS;
    size_t maxBuffersPerClass = DEFAULT_MAX_BUFFERS_PER_CLASS;
};

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

    /// Hide the BufferManager constructor and only allow creation via BufferManager::create().
    /// Following: https://en.cppreference.com/w/cpp/memory/enable_shared_from_this
    struct Private
    {
        explicit Private() = default;
    };

public:
    static constexpr auto DEFAULT_BUFFER_SIZE = 8 * 1024;
    static constexpr auto DEFAULT_NUMBER_OF_BUFFERS = 1024;
    static constexpr auto DEFAULT_ALIGNMENT = 64;

    explicit BufferManager(
        Private,
        uint32_t bufferSize,
        uint32_t numOfBuffers,
        std::shared_ptr<std::pmr::memory_resource> memoryResource,
        std::optional<SizeClassConfig> sizeClasses);

    /// Creates a new global buffer manager. Buffers are always aligned to DEFAULT_ALIGNMENT (a cache line).
    /// @param bufferSize the size of each default-class buffer in bytes
    /// @param numOfBuffers the total number of default-class buffers in the pool
    /// @param memoryResource resource for allocating and deallocating memory
    /// @param sizeClasses optional configuration enabling additional power-of-two size classes for variable-sized pooled buffers
    /// NOLINTBEGIN(fuchsia-default-arguments-declarations): defaults match the established BufferManager::create API on main.
    static std::shared_ptr<BufferManager> create(
        uint32_t bufferSize = DEFAULT_BUFFER_SIZE,
        uint32_t numOfBuffers = DEFAULT_NUMBER_OF_BUFFERS,
        const std::shared_ptr<std::pmr::memory_resource>& memoryResource = std::make_shared<NesDefaultMemoryAllocator>(),
        std::optional<SizeClassConfig> sizeClasses = std::nullopt);
    /// NOLINTEND(fuchsia-default-arguments-declarations)

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager() override;

    BufferManagerType getBufferManagerType() const override;

private:
    /**
     * @brief Build the size class pools (the default class plus any configured power-of-two classes)
     * and preallocate their buffers according to the provisioning policy. One shot call.
     */
    void initialize(const std::optional<SizeClassConfig>& sizeClasses);

    /// Index of the smallest size class whose buffers are >= size, or pools.size() if none fits (too large).
    [[nodiscard]] size_t classIndexForSize(size_t size) const noexcept;

    /// Prepares the segment's control block (reference count 0 -> 1) and wraps it in a TupleBuffer.
    TupleBuffer wrapSegment(NES::detail::MemorySegment* segment);

public:
    /// This blocks until a buffer is available.
    TupleBuffer getBufferBlocking() override;

    /// invalid optional if there is no buffer.
    std::optional<TupleBuffer> getBufferNoBlocking() override;

    /// Blocks until a pooled buffer of at least `size` bytes is available, served from the smallest
    /// fitting size class. Requests larger than the largest class fall back to an unpooled buffer.
    TupleBuffer getBuffer(size_t size) override;

    /// Non-blocking variant of getBuffer(size).
    std::optional<TupleBuffer> getBufferNoBlocking(size_t size) override;

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
    /// One pool per size class, sorted ascending by buffer size. pools[defaultPoolIndex] is the
    /// class of exactly `bufferSize` that backs the no-arg getBufferBlocking()/getBufferSize().
    std::vector<std::unique_ptr<NES::detail::FixedSizeClassPool>> pools;
    size_t defaultPoolIndex{0};

    std::shared_ptr<NES::UnpooledChunksManager> unpooledChunksManager;

    size_t bufferSize;
    size_t numOfBuffers;
    uint32_t alignment{DEFAULT_ALIGNMENT};

    std::shared_ptr<std::pmr::memory_resource> memoryResource;
    std::atomic<bool> isDestroyed{false};
};


}
