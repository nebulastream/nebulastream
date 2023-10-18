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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_THREADLOCALSLICESTORE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_THREADLOCALSLICESTORE_HPP_

#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <folly/Synchronized.h>
#include <list>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class NonKeyedSlice;
using GlobalSlicePtr = std::unique_ptr<NonKeyedSlice>;

/**
* @brief A Slice store for tumbling and sliding windows,
* which stores slices for a specific thread.
* In the current implementation we handle tumbling windows as sliding widows with windowSize==windowSlide.
* As the slice store is only using by a single thread, we dont have to protect its functions for concurrent accesses.
*/
template<class SliceType>
class ThreadLocalSliceStore {
  public:
    using SliceTypePtr = std::unique_ptr<SliceType>;
    explicit ThreadLocalSliceStore(uint64_t windowSize, uint64_t windowSlide) : windowAssigner(windowSize, windowSlide){};
    virtual ~ThreadLocalSliceStore() = default;

    /**
     * @brief Retrieves a slice which covers a specific ts.
     * If no suitable slice is contained in the slice store we add a slice at the correct position.
     * Slices cover a cover a range from [startTs, endTs - 1]
     * @param timestamp requires to be larger then the lastWatermarkTs
     * @return KeyedSlicePtr
     */
    SliceTypePtr& findSliceByTs(uint64_t ts);

    /**
     * @brief Returns the slice end ts for the first slice in the slice store.
     * @return uint64_t
     */
    inline SliceTypePtr& getFirstSlice();

    /**
     * @brief Returns the slice end ts for the last slice in the slice store.
     * @return uint64_t
     */
    inline SliceTypePtr& getLastSlice();

    std::list<std::shared_ptr<SliceType>> extractSlicesUntilTs(uint64_t ts);

    /**
     * @brief Deletes the slice with the smalles slice index.
     */
    void removeSlicesUntilTs(uint64_t ts);

    /**
     * @brief Returns the last watermark.
     * @return uint64_t
     */
    uint64_t getLastWatermark() { return lastWatermarkTs; }

    /**
     * @brief Sets the last watermark
     * @param watermarkTs
     */
    void setLastWatermark(uint64_t watermarkTs) { this->lastWatermarkTs = watermarkTs; }

    /**
     * @brief Returns the number of currently stored slices
     * @return uint64_t
     */
    uint64_t getNumberOfSlices() {
        auto lock = synchronizedSlices.rlock();
        return lock->size();
    }

    /**
     * @brief Gets an unprotected reference to all slices
     * @return std::list<KeyedSlicePtr>
     */
    auto& getSlices() {
        auto lock = synchronizedSlices.rlock();
        return lock.asNonConstUnsafe();
    }

  private:
    virtual SliceTypePtr allocateNewSlice(uint64_t startTs, uint64_t endTs) = 0;

  private:
    const SliceAssigner windowAssigner;
    folly::Synchronized<std::list<SliceTypePtr>> synchronizedSlices;
    std::atomic<uint64_t> lastWatermarkTs = 0;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_THREADLOCALSLICESTORE_HPP_
