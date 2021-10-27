/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_RUNTIME_LOCAL_BUFFER_POOL_HPP_
#define NES_INCLUDE_RUNTIME_LOCAL_BUFFER_POOL_HPP_

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferRecycler.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <deque>
#include <memory>
#include <mutex>
#ifdef NES_USE_LATCH_FREE_BUFFER_MANAGER
#include <folly/MPMCQueue.h>
#include <folly/concurrency/UnboundedQueue.h>
#endif

namespace NES {

namespace Runtime {
class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;
class TupleBuffer;
namespace detail {
class MemorySegment;
}

/**
 * @brief A local buffer pool that uses N exclusive buffers and then falls back to the global buffer manager
 */
class LocalBufferPool : public BufferRecycler, public AbstractBufferProvider {
  public:
    /**
     * @brief Construct a new LocalBufferPool
     * @param bufferManager the global buffer manager
     * @param availableBuffers deque of exclusive buffers
     * @param numberOfReservedBuffers number of exclusive bufferss
     */
    explicit LocalBufferPool(const BufferManagerPtr& bufferManager,
                             std::deque<detail::MemorySegment*>&& availableBuffers,
                             size_t numberOfReservedBuffers);
    ~LocalBufferPool() override;

    /**
     * @brief Destroys this buffer pool and returns own buffers to global pool
     */
    void destroy() override;

    /**
    * @brief Provides a new TupleBuffer. This blocks until a buffer is available.
    * @return a new buffer
    */
    TupleBuffer getBufferBlocking() override;
    size_t getBufferSize() const override;
    size_t getNumOfPooledBuffers() const override;
    size_t getNumOfUnpooledBuffers() const override;
    std::optional<TupleBuffer> getBufferNoBlocking() override;
    std::optional<TupleBuffer> getBufferTimeout(std::chrono::milliseconds timeout_ms) override;
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

  private:
    std::shared_ptr<BufferManager> bufferManager;
#ifndef NES_USE_LATCH_FREE_BUFFER_MANAGER
    std::deque<detail::MemorySegment*> exclusiveBuffers;
#else
    folly::MPMCQueue<detail::MemorySegment*> exclusiveBuffers;
#endif
#ifdef NES_DEBUG_TUPLE_BUFFER_LEAKS
    std::vector<detail::MemorySegment*> allSegments;
#endif
    size_t numberOfReservedBuffers;
    mutable std::mutex mutex;
};

}// namespace Runtime

}// namespace NES

#endif  // NES_INCLUDE_RUNTIME_LOCAL_BUFFER_POOL_HPP_
