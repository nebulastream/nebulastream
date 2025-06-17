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

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <folly/MPMCQueue.h>

namespace NES::Memory
{

const std::chrono::seconds DEFAULT_BUFFER_TIMEOUT = std::chrono::seconds(10);

/**
 * @brief A local buffer pool that uses N exclusive buffers and then falls back to the global buffer manager
 */
class FixedSizeBufferPool : public BufferRecycler, public AbstractBufferProvider, public std::enable_shared_from_this<FixedSizeBufferPool>
{
public:
    /**
     * @brief Construct a new LocalBufferPool
     * @param bufferManager the global buffer manager
     * @param availableBuffers deque of exclusive buffers
     * @param numberOfReservedBuffers number of exclusive buffers
     */
    explicit FixedSizeBufferPool(
        const std::shared_ptr<BufferManager>& bufferManager,
        std::deque<detail::MemorySegment*>& availableBuffers,
        size_t numberOfReservedBuffers);

    ~FixedSizeBufferPool() override;

    /**
     * @brief Destroys this buffer pool and returns own buffers to global pool
     */
    void destroy() override;

    /**
    * @brief Provides a new TupleBuffer. This blocks until a buffer is available.
    * @return a new buffer
    */
    TupleBuffer getBufferBlocking() override;

    /**
     * @brief Returns a new Buffer wrapped in an optional or an invalid option if there is no buffer available within
     * timeout_ms.
     * @param timeout_ms the amount of time to wait for a new buffer to be retuned
     * @return a new buffer
     */
    std::optional<TupleBuffer> getBufferWithTimeout(std::chrono::milliseconds timeout) override;
    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    size_t getTotalSizeOfUnpooledBufferChunks() const override;
    std::optional<TupleBuffer> getBufferNoBlocking() override;
    std::optional<TupleBuffer> getUnpooledBuffer(size_t bufferSize) override;
    /**
     * @brief provide number of available exclusive buffers
     * @return number of available exclusive buffers
     */
    size_t getAvailableBuffers() const override;

    /**
     * @brief Recycle a pooled buffer that is might be exclusive to the pool
     * @param buffer
     */
    void recyclePooledBuffer(detail::MemorySegment* memSegment) override;

    /**
     * @brief This calls is not supported and raises Runtime error
     * @param buffer
     */
    void recycleUnpooledBuffer(detail::MemorySegment* buffer) override;

    virtual BufferManagerType getBufferManagerType() const override;

    std::shared_ptr<BufferManager> getParentBufferManager() const { return bufferManager; }

private:
    std::shared_ptr<BufferManager> bufferManager;

    folly::MPMCQueue<detail::MemorySegment*> exclusiveBuffers;
    [[maybe_unused]] size_t numberOfReservedBuffers;
    mutable std::mutex mutex;
    std::condition_variable cvar;
    std::atomic<bool> isDestroyed;
};

}
