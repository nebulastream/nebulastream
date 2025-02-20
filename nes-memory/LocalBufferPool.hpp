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

#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/DataSegment.hpp>
#include <folly/MPMCQueue.h>

namespace NES::Memory
{

/**
 * @brief A local buffer pool that uses N exclusive buffers and then falls back to the global buffer manager
 */
class LocalBufferPool : public BufferRecycler, public AbstractBufferProvider
{
public:
    /**
     * @brief Construct a new LocalBufferPool
     * @param bufferManager the global buffer manager
     * @param availableBuffers deque of exclusive buffers
     * @param numberOfReservedBuffers number of exclusive bufferss
     * @param deallocator functor to free memory that was not a preAllocatedBlock (previously "unpooled" segments)
     */
    explicit LocalBufferPool(
        const std::shared_ptr<BufferManager>& bufferManager,
        std::deque<detail::DataSegment<detail::InMemoryLocation>>& availableBuffers,
        size_t numberOfReservedBuffers,
        const std::function<void(detail::DataSegment<detail::InMemoryLocation>&&)>& deallocator);
    ~LocalBufferPool() override;

    /**
     * @brief Destroys this buffer pool and returns own buffers to global pool
     */
    void destroy() override;

    /**
    * @brief Provides a new TupleBuffer. This blocks until a buffer is available.
    * @return a new buffer
    */
    PinnedBuffer getBufferBlocking() override;
    RepinBufferFuture repinBuffer(FloatingBuffer&&) noexcept override;
    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    std::optional<PinnedBuffer> getBufferNoBlocking() override;
    std::optional<PinnedBuffer> getBufferWithTimeout(std::chrono::milliseconds timeout_ms) override;
    std::optional<PinnedBuffer> getUnpooledBuffer(size_t bufferSize) override;
    /**
     * @brief provide number of available exclusive buffers
     * @return number of available exclusive buffers
     */
    size_t getAvailableBuffers() const override;

    /**
     * @brief Recycle a pooled buffer that is might be exclusive to the pool
     * @param buffer
     */
    void recycleSegment(detail::DataSegment<detail::InMemoryLocation>&& memSegment) override;


    /**
     * @brief Recycle a pooled buffer that is might be exclusive to the pool
     * @param buffer
     */
    bool recycleSegment(detail::DataSegment<detail::OnDiskLocation>&& memSegment) override;


    virtual BufferManagerType getBufferManagerType() const override;

private:
    std::shared_ptr<BufferManager> bufferManager;
    const std::function<void(detail::DataSegment<detail::InMemoryLocation>)> deallocator;

    folly::MPMCQueue<detail::DataSegment<detail::InMemoryLocation>> exclusiveBuffers;
    mutable std::mutex allBuffersMutex;
    //TODO erase and shrink allBuffers periodically
    std::vector<detail::BufferControlBlock*> allBuffers;
    std::atomic<uint32_t> exclusiveBufferCount;
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    std::vector<detail::MemorySegment*> allSegments;
#endif
    size_t numberOfReservedBuffers;
    mutable std::mutex mutex;
};

}
