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

#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
#include <Exceptions/WindowProcessingException.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <list>
#include <map>
#include <memory>

namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
}// namespace NES::Experimental

namespace NES::Windowing::Experimental {

class KeyedSlice;
using KeyedSlicePtr = std::unique_ptr<KeyedSlice>;

/**
 * @brief A Slice store for tumbling and sliding windows,
 * which stores slices for a specific thread.
 * In the current implementation we handle tumbling windows as sliding widows with windowSize==windowSlide.
 * As the slice store is only using by a single thread, we dont have to protect its functions for concurrent accesses.
 */
class KeyedThreadLocalSliceStore {
  public:
    explicit KeyedThreadLocalSliceStore(NES::Experimental::HashMapFactoryPtr hashMapFactory,
                                        uint64_t windowSize,
                                        uint64_t windowSlide,
                                        uint64_t numberOfPreallocatedSlices);

    /**
     * @brief Calculates the start of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the start of the particular slice.
     * @return uint64_t slice start
     */
    inline uint64_t getSliceStartTs(uint64_t ts) {
        auto nextWindowStart = ts - (ts) % windowSize;
        auto nextSlideStart = ts - (ts) % windowSlide;
        return std::max(nextWindowStart, nextSlideStart);
    }

    /**
     * @brief Calculates the end of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the end of the particular slice.
     * @return uint64_t slice end
     */
    inline uint64_t getSliceEndTs(uint64_t ts) {
        auto nextWindowStart = ts + (windowSize - (ts) % windowSize);
        auto nextSlideStart = ts + (windowSlide - (ts) % windowSlide);
        return std::min(nextWindowStart, nextSlideStart);
    }

    /**
     * @brief Finds a slice by a specific time stamp.
     * @param ts timestamp
     * @return KeyedSlicePtr
     */
    inline KeyedSlicePtr& findSliceByTs(uint64_t ts) {
        NES_ASSERT(ts > lastWatermarkTs, "The ts " << ts << " can't be smaller then the lastWatermarkTs " << lastWatermarkTs);
        // calculate the slice end for a specific ts
        auto sliceEndTs = getSliceEndTs(ts);
        return getSlice(sliceEndTs);
    }

    /**
     * @brief Returns an slice by a specific slice end timestamp.
     * @param sliceEndTs
     * @return KeyedSlicePtr
     */
    inline const KeyedSlicePtr& operator[](uint64_t sliceEndTs) { return getSlice(sliceEndTs); }

    /**
     * @brief Returns the slice end ts for the first slice in the slice store.
     * @return uint64_t
     */
    inline uint64_t getFirstIndex() { return minimalSliceEnd; }

    /**
     * @brief Returns the slice end ts for the last slice in the slice store.
     * @return uint64_t
     */
    inline uint64_t getLastIndex() { return currentSlice->getEnd(); }

    /**
     * @brief Deletes the slice with the smalles slice index.
     */
    void dropFirstSlice();

    /**
     * @brief Returns the last watermark.
     * @return uint64_t
     */
    uint64_t getLastWatermark();

    /**
     * @brief Sets the last watermark
     * @param watermarkTs
     */
    void setLastWatermark(uint64_t watermarkTs);

    /**
     * @brief Sets the first slice index.
     * @param slice index
     */
    void setFirstSliceIndex(uint64_t sliceIndex);

    /**
     * @brief Returns the number of currently stored slices
     * @return uint64_t
     */
    uint64_t getNumberOfSlices();

  private:
    /**
     * @brief Appends a new slice to the end of the slice store.
     * @throws WindowProcessingException if the slice store is full
     */
    KeyedSlicePtr& insertSlice(uint64_t sliceIndex);

    /**
     * @brief Get a specific slice at a specific slice index.
     * @param sliceIndex
     * @return KeyedSlicePtr
     */
    inline KeyedSlicePtr& getSlice(uint64_t sliceEnd) {

        if (slices.empty()) {
        }

        auto sliceIter = slices.rbegin();
        while ((*sliceIter)->getStart() > sliceEnd) {
            sliceIter++;
        }

        auto& slice = *sliceIter;
        if(slice->getStart() <= sliceEnd && slice->getEnd() < sliceEnd){
            // we found the correct slice
        }
        auto s = allocateNewSlice(0,0,0);
        slices.emplace(sliceIter.base(), std::move(s));
        sliceIter.add

        if (currentSlice != nullptr && sliceEnd == lastIndex) {
            return currentSlice;
        } else if (!slices.contains(sliceEnd)) {
            return insertSlice(sliceEnd);
        } else {
            return slices[sliceEnd];
        }
    }

    KeyedSlicePtr allocateNewSlice(uint64_t startTs, uint64_t endTs, uint64_t sliceIndex);

  private:
    NES::Experimental::HashMapFactoryPtr hashMapFactory;
    const uint64_t windowSlide;
    const uint64_t windowSize;
    KeyedSlicePtr currentSlice;
    std::list<KeyedSlicePtr> slices;
    uint64_t firstIndex = 0;
    uint64_t lastIndex = 0;
    uint64_t lastWatermarkTs = 0;
};
}// namespace NES::Windowing::Experimental
#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
