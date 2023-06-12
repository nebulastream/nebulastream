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
#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_THREADLOCALSLICESTORE_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_THREADLOCALSLICESTORE_HPP_

#include <Exceptions/WindowProcessingException.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/SlidingWindowAssigner.hpp>
#include <list>
#include <memory>

namespace NES::Windowing::Experimental {

class GlobalSlice;
using GlobalSlicePtr = std::unique_ptr<GlobalSlice>;

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
    SliceTypePtr& findSliceByTs(uint64_t ts) {
        if (ts < lastWatermarkTs) {
            throw new WindowProcessingException("The ts " + std::to_string(ts) + " can't be smaller then the lastWatermarkTs "
                                                + std::to_string(lastWatermarkTs));
        }
        // Find the correct slice.
        // Reverse iteration over all slices from the end to the start,
        // as it is expected that ts is in a more recent slice.
        // At the end of the iteration sliceIter can be in three states:
        // 1. sliceIter == slices.rend() -> their is no slice with a start <= ts.
        // In this case we have to pre-pend a new slice.
        // 2. sliceIter is at a slide which slice.startTs <= ts and slice.endTs < ts.
        // In this case we have to insert a new slice after the current sliceIter
        // 3. sliceIter is at a slide which slice.startTs <= ts and slice.endTs > ts.
        // In this case we found the correct slice.
        auto sliceIter = slices.rbegin();
        while (sliceIter != slices.rend() && (*sliceIter)->getStart() > ts) {
            sliceIter++;
        }
        // Handle the individual cases and append a slice if required.
        if (sliceIter == slices.rend()) {
            // We are in case 1. thus we have to prepend a new slice
            auto newSliceStart = windowAssigner.getSliceStartTs(ts);
            auto newSliceEnd = windowAssigner.getSliceEndTs(ts);
            auto newSlice = allocateNewSlice(newSliceStart, newSliceEnd);
            return slices.emplace_front(std::move(newSlice));
        } else if ((*sliceIter)->getStart() < ts && (*sliceIter)->getEnd() <= ts) {
            // We are in case 2. thus we have to append a new slice after the current iterator
            auto newSliceStart = windowAssigner.getSliceStartTs(ts);
            auto newSliceEnd = windowAssigner.getSliceEndTs(ts);
            auto newSlice = allocateNewSlice(newSliceStart, newSliceEnd);
            auto slice = slices.emplace(sliceIter.base(), std::move(newSlice));
            return *slice;
        } else if ((*sliceIter)->coversTs(ts)) {
            // We are in case 3. and found a slice which covers the current ts.
            // Thus, we return a reference to the slice.
            return *sliceIter;
        } else {
            NES_THROW_RUNTIME_ERROR("Error during slice lookup: We looked for ts: " << ts << " current front: "
                                                                                    << getLastSlice()->getStart() << " - "
                                                                                    << getLastSlice()->getEnd());
        }
    }

    ;

    /**
     * @brief Returns the slice end ts for the first slice in the slice store.
     * @return uint64_t
     */
    inline SliceTypePtr& getFirstSlice() { return slices.front(); }

    /**
     * @brief Returns the slice end ts for the last slice in the slice store.
     * @return uint64_t
     */
    inline SliceTypePtr& getLastSlice() { return slices.back(); }

    /**
     * @brief Deletes the slice with the smalles slice index.
     */
    void removeSlicesUntilTs(
        uint64_t ts) {// drop all slices as long as the list is not empty and the first slice ends before the current ts.
        while (!slices.empty() && slices.front()->getEnd() <= ts) {
            slices.pop_front();
        }
    }

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
    uint64_t getNumberOfSlices() { return slices.size(); }

    /**
     * @brief Gets a reference to all slices
     * @return std::list<KeyedSlicePtr>
     */
    auto& getSlices() { return slices; }

  private:
    virtual SliceTypePtr allocateNewSlice(uint64_t startTs, uint64_t endTs) = 0;

  private:
    const SlidingWindowAssigner windowAssigner;
    std::list<SliceTypePtr> slices;
    uint64_t lastWatermarkTs = 0;
};
}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_THREADLOCALSLICESTORE_HPP_
