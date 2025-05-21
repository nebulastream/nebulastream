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
#include <cstdint>
#include <deque>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Util/RollingAverage.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>

namespace NES::Memory
{

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
 */
class BufferManager : public std::enable_shared_from_this<BufferManager>,
                      public BufferRecycler,
                      public AbstractBufferProvider,
                      public AbstractPoolProvider
{
    friend class TupleBuffer;
    friend class detail::MemorySegment;
    /// Hide the BufferManager constructor and only allow creation via BufferManager::create().
    /// Following: https://en.cppreference.com/w/cpp/memory/enable_shared_from_this
    struct Private
    {
        explicit Private() = default;
    };

    static constexpr auto DEFAULT_BUFFER_SIZE = 8 * 1024;
    static constexpr auto DEFAULT_NUMBER_OF_BUFFERS = 1024;
    static constexpr auto DEFAULT_ALIGNMENT = 64;

public:
    explicit BufferManager(
        Private,
        uint32_t bufferSize,
        uint32_t numOfBuffers,
        std::shared_ptr<std::pmr::memory_resource> memoryResource,
        uint32_t withAlignment);

    /**
     * @brief Creates a new global buffer manager
     * @param bufferSize the size of each buffer in bytes
     * @param numOfBuffers the total number of buffers in the pool
     * @param withAlignment the alignment of each buffer, default is 64 so ony cache line aligned buffers, This value must be a pow of two and smaller than page size
     */
    static std::shared_ptr<BufferManager> create(
        uint32_t bufferSize = DEFAULT_BUFFER_SIZE,
        uint32_t numOfBuffers = DEFAULT_NUMBER_OF_BUFFERS,
        std::shared_ptr<std::pmr::memory_resource> memoryResource = std::make_shared<NesDefaultMemoryAllocator>(),
        uint32_t withAlignment = DEFAULT_ALIGNMENT);

    BufferManager(const BufferManager&) = delete;
    BufferManager& operator=(const BufferManager&) = delete;
    ~BufferManager() override;

    BufferManagerType getBufferManagerType() const override;

private:
    /**
     * @brief Configure the BufferManager to use numOfBuffers buffers of size bufferSize bytes.
     * This is a one shot call. A second invocation of this call will fail
     * @param withAlignment
     */
    void initialize(uint32_t withAlignment);

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

    /**
     * @brief Returns an unpooled buffer of size bufferSize wrapped in an optional or an invalid option if an error
     * occurs.
     * @param bufferSize
     * @return a new buffer
     */
    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override;


    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    size_t getAvailableBuffers() const override;
    size_t getAvailableBuffersInFixedSizePools() const;
    size_t getSizeOfUnpooledBufferChunks() const;

    /**
      * @brief Create a local buffer manager that is assigned to one pipeline or thread
      * @param numberOfReservedBuffers number of exclusive buffers to give to the pool
      * @return a fixed buffer manager with numberOfReservedBuffers exclusive buffer
      */
    std::optional<std::shared_ptr<AbstractBufferProvider>> createFixedSizeBufferPool(size_t numberOfReservedBuffers) override;

    /**
     * @brief Recycle a pooled buffer by making it available to others
     * @param buffer
     */
    void recyclePooledBuffer(detail::MemorySegment* segment) override;

    /**
    * @brief Recycle an unpooled buffer by making it available to others
    * @param buffer
    */
    void recycleUnpooledBuffer(detail::MemorySegment* segment) override;

    /**
     * @brief this method clears all local buffers pools and remove all buffers from the global buffer manager
     */
    void destroy() override;

private:
    std::vector<detail::MemorySegment> allBuffers;

    folly::MPMCQueue<detail::MemorySegment*> availableBuffers;
    std::atomic<size_t> numOfAvailableBuffers;

    struct UnpooledChunk
    {
        size_t totalSize = 0;
        size_t usedSize = 0;
        uint8_t* startOfChunk = nullptr;
        std::vector<std::unique_ptr<detail::MemorySegment>> unpooledMemorySegments;
        uint64_t activeMemorySegments = 0;
    };

    /// During initialize, we create one unpooled chunk with the buffer size

    std::unordered_map<uint8_t*, UnpooledChunk> unpooledBufferChunkStorage;
    static constexpr auto NUM_PRE_ALLOCATED_CHUNKS = 100;
    static constexpr auto ROLLING_AVERAGE_UNPOOLED_BUFFER_SIZE = 100;
    uint8_t* lastAllocateChunkPtr;
    folly::Synchronized<RollingAverage<size_t>> rollingAverage;
    mutable std::recursive_mutex unpooledBuffersMutex;
    std::atomic<size_t> unpooledBufferChunksSize;

    mutable std::recursive_mutex availableBuffersMutex;
    std::condition_variable_any availableBuffersCvar;

    size_t bufferSize;
    size_t numOfBuffers;

    uint8_t* basePointer{nullptr};
    size_t allocatedAreaSize;


    /// A BufferManager may create smaller localBufferPools.
    /// However, it should not directly own the local buffer pools, but instead leave the ownership to what ever component uses
    /// the localBufferPool. The BufferManager does require a reference to the localBufferPools to destroy them once the
    /// BufferManager is destroyed. Destroying the BufferManager potentially creates destroyed local buffer pools which are
    /// safe to access, but will no longer be able to allocate buffers.
    std::vector<std::weak_ptr<AbstractBufferProvider>> localBufferPools;
    mutable std::recursive_mutex localBufferPoolsMutex;

    std::shared_ptr<std::pmr::memory_resource> memoryResource;
    std::atomic<bool> isDestroyed{false};
};


}
