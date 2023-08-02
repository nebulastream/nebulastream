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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICESTORE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICESTORE_HPP_

#include <Execution/Operators/Streaming/Aggregations/WindowProcessingException.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <list>
#include <memory>
#include <vector>
#include <set>

namespace NES::Runtime::Execution::Operators {

class NonKeyedSlice;
using GlobalSlicePtr = std::unique_ptr<NonKeyedSlice>;

/**
* @brief The global slice store maintains a set of slices for all in-flight sliding windows.
*/
template<class SliceType>
class SlidingWindowSliceStore {
  public:
    using SliceTypePtr = std::shared_ptr<SliceType>;
    explicit SlidingWindowSliceStore(uint64_t windowSize, uint64_t windowSlide)
        : windowSize(windowSize), windowSlide(windowSlide){};
    virtual ~SlidingWindowSliceStore() = default;

    /**
     * @brief Inserts a new slice to the slice store.
     */
    void insertSlice(SliceTypePtr slice) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        auto sliceEnd = slice->getEnd();
        auto sliceStart = slice->getStart();
        auto sliceIter = slices.rbegin();
        // reverse iterate from the end of the slice store to the front as long as the slice.start > currentSlice.start
        while (sliceIter != slices.rend() && (*sliceIter)->getStart() > sliceStart) {
            sliceIter++;
        }
        if (sliceIter == slices.rend()) {
            // we reached the front of all slices -> so insert in the front of the slice store
            slices.emplace_front(std::move(slice));
        } else if ((*sliceIter)->getStart() < sliceStart) {
            slices.emplace(sliceIter.base(), std::move(slice));
        } else {
            throw WindowProcessingException("Could not find the correct slot in the global slice store.");
        }
    }

    /**
     * @brief Returns the slice end ts for the first slice in the slice store.
     * @return uint64_t
     */
    SliceTypePtr& getFirstSlice() {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        return slices.front();
    }

    /**
     * @brief Returns the slice end ts for the last slice in the slice store.
     * @return uint64_t
     */
    SliceTypePtr& getLastSlice() {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        return slices.back();
    }

    std::set<std::tuple<uint64_t, uint64_t>> collectWindows(uint64_t oldTs, uint64_t currentTs) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        std::set<std::tuple<uint64_t, uint64_t>> windows;
        // collect all windows, for which the list of slices contains the slice end.
        // iterate over all slices that would open a window that ends before endTs.
        for (auto& slice : slices) {
            if (slice->getEnd() > currentTs) {
                break;
            }
            if (slice->getEnd() >= oldTs && slice->getEnd() <= currentTs) {
                auto windowStart = slice->getStart();
                auto windowEnd = slice->getStart() + windowSize;
                if ((windowEnd > oldTs) && (windowEnd <= currentTs) && ((windowStart % windowSlide) == 0)) {
                    windows.insert(std::make_tuple(windowStart, windowEnd));
                }
                windowStart = slice->getEnd();
                windowEnd = slice->getEnd() + windowSize;
                if ((windowEnd > oldTs) && (windowEnd <= currentTs) && ((windowStart % windowSlide) == 0)) {
                    windows.insert(std::make_tuple(windowStart, windowEnd));
                }
            }
        }
        return windows;
    }

    std::list<std::tuple<uint64_t, uint64_t>> collectAllWindows(uint64_t oldTs) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        std::list<std::tuple<uint64_t, uint64_t>> windows;
        // collect all windows, for which the list of slices contains the slice end.
        // iterate over all slices that would open a window that ends before endTs.
        for (auto& slice : slices) {
            if (slice->getEnd() >= oldTs) {
                uint64_t windowStart = slice->getStart();
                auto windowEnd = slice->getStart() + windowSize;
                // check if it is a valid window
                if ((oldTs < windowEnd) && ((windowStart % windowSlide) == 0)) {
                    windows.emplace_back(std::make_tuple(windowStart, windowEnd));
                }
            }
        }
        return windows;
    }

    std::vector<SliceTypePtr> collectSlicesForWindow(uint64_t windowStart, uint64_t windowEnd) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        std::vector<SliceTypePtr> windowSlices;
        // iterate over all slices that are contained in the window that ends before endTs.
        for (auto& slice : slices) {
            if (slice->getStart() > windowEnd) {
                break;
            }
            if (slice->getStart() >= windowStart && slice->getEnd() <= windowEnd) {
                windowSlices.emplace_back(slice);
            }
        }
        return windowSlices;
    }

    /**
     * @brief Remove all slices from the slice store that are not required for any windows anymore
     * @param ts
     */
    void removeSlices(uint64_t ts) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);

        uint64_t maxWindowEndTs = ts;
        if (__builtin_sub_overflow(maxWindowEndTs, windowSlide, &maxWindowEndTs)) {
            maxWindowEndTs = 0;
        }
        while (!slices.empty() && slices.front()->getEnd() <= maxWindowEndTs) {
            slices.pop_front();
        }
    }

    /**
     * @brief Returns the number of currently stored slices
     * @return uint64_t
     */
    uint64_t getNumberOfSlices() {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        return slices.size();
    }

  private:
    uint64_t windowSize;
    uint64_t windowSlide;
    std::mutex sliceStagingMutex;
    std::list<SliceTypePtr> slices;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICESTORE_HPP_