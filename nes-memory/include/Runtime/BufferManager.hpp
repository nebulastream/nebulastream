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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
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
        uint32_t alignment);

    /// Creates a new global buffer manager from a total memory budget. The pooled buffer count and the unpooled
    /// memory limit are derived: unpooledLimit = totalMemoryInBytes * unpooledMemoryFraction, the remaining
    /// pooled bytes are split into floor(pooledBytes / bufferSize) buffers.
    /// @param totalMemoryInBytes total memory budget shared by the pooled and unpooled pools
    /// @param unpooledMemoryFraction share of the total budget reserved for unpooled buffers, in [0.0, 1.0]
    /// @param alignment byte alignment of every buffer; must be a power of two <= page size (a cache line is 64 bytes)
    /// @param bufferSize the size of each pooled buffer in bytes
    /// @param memoryResource resource for allocating and deallocating memory
    static std::shared_ptr<BufferManager> create(
        size_t totalMemoryInBytes,
        double unpooledMemoryFraction,
        BufferAlignment alignment,
        uint32_t bufferSize,
        const std::shared_ptr<std::pmr::memory_resource>& memoryResource);

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager() override;

    BufferManagerType getBufferManagerType() const override;

private:
    /**
     * @brief Configure the BufferManager to use numOfBuffers buffers of size bufferSize bytes, aligned to
     * a cache line. This is a one shot call. A second invocation of this call will fail
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
    std::vector<NES::detail::MemorySegment> allBuffers;

    folly::MPMCQueue<NES::detail::MemorySegment*> availableBuffers;

    std::shared_ptr<NES::UnpooledChunksManager> unpooledChunksManager;

    size_t bufferSize;
    size_t numOfBuffers;
    uint32_t alignment;

    uint8_t* basePointer{nullptr};
    size_t allocatedAreaSize;

    std::shared_ptr<std::pmr::memory_resource> memoryResource;
    std::atomic<bool> isDestroyed{false};
};


}
