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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALSLICESTORE_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALSLICESTORE_HPP_
#include <Exceptions/WindowProcessingException.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/SeqenceLog.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Windowing/Watermark/WatermarkProcessor.hpp>
#include <cinttypes>
#include <list>
#include <map>
#include <memory>
#include <mutex>
namespace NES::Windowing::Experimental {

class KeyedSlice;
using KeyedSliceSharedPtr = std::shared_ptr<KeyedSlice>;

/**
 * @brief The global slice store that contains pre-aggregated slice.
 * After appending these slices to the global slice store, they are implicitly immutable.
 * Thus we can concurrently, read them from multiple threads.
 */
template<typename SliceType>
class GlobalSliceStore {
  public:
    using SliceTypeSharedPtr = std::shared_ptr<SliceType>;
    ~GlobalSliceStore() {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        NES_DEBUG("~GlobalSliceStore")
        slices.clear();
    }

    /**
     * @brief Add a new slice to the slice store and trigger all windows, which trigger, because we added this slice.
     * @param sequenceNumber index of the slice
     * @param slice the slice, which we want to add to the slice store
     * @param windowSize the window size
     * @param windowSlide the window slide
     * @return std::vector<Window>
     */
    std::vector<Window>
    addSliceAndTriggerWindows(uint64_t sequenceNumber, SliceTypeSharedPtr slice, uint64_t windowSize, uint64_t windowSlide) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        auto sliceEnd = slice->getEnd();
        auto sliceStart = slice->getStart();
        auto sliceIter = slices.rbegin();
        while (sliceIter != slices.rend() && (*sliceIter)->getStart() > sliceStart) {
            sliceIter++;
        }
        if (sliceIter == slices.rend()) {
            slices.emplace_front(std::move(slice));
        } else if ((*sliceIter)->getStart() < sliceStart) {
            slices.emplace(sliceIter.base(), std::move(slice));
        } else {
            throw WindowProcessingException("there is no slice contained in the global slice store.");
        }
        auto lastMaxSliceEnd = sliceAddSequenceLog.getCurrentWatermark();
        sliceAddSequenceLog.updateWatermark(sliceEnd, sequenceNumber);
        auto newMaxSliceEnd = sliceAddSequenceLog.getCurrentWatermark();
        return triggerInflightWindows(windowSize, windowSlide, lastMaxSliceEnd, newMaxSliceEnd);
    }

    /**
     * @brief Trigger all inflight window, which are covered by at least one slice in the the slice store
     * @param windowSize the window size
     * @param windowSlide the window slide
     * @return std::vector<Window>
     */
    std::vector<Window> triggerAllInflightWindows(uint64_t windowSize, uint64_t windowSlide) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        auto lastMaxSliceEnd = sliceAddSequenceLog.getCurrentWatermark();
        return triggerInflightWindows(windowSize, windowSlide, lastMaxSliceEnd, slices.back()->getEnd() + windowSize);
    }

    /**
     * @brief Triggers a specific slice and garbage collects all slices
     * that are not necessary any more.
     * @param sequenceNumber
     * @param sliceIndex
     */
    void finalizeSlice(uint64_t sequenceNumber, uint64_t sliceIndex) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);

        // calculate the slice which could potentially be finalized according to the given slice index.
        // this is given by current sliceIndex - slicesPerWindow
        // to prevent overflows we use __builtin_sub_overflow to check if an overflow happened
        uint64_t finalizeSliceIndex;
        if (__builtin_sub_overflow(sliceIndex, 0, &finalizeSliceIndex)) {
            finalizeSliceIndex = 0;
        }

        auto lastFinalizedSliceIndex = sliceAddSequenceLog.getCurrentWatermark();
        sliceTriggerSequenceLog.updateWatermark(finalizeSliceIndex, sequenceNumber);
        auto newFinalizedSliceIndex = sliceAddSequenceLog.getCurrentWatermark();

        // remove all slices which are between lastFinalizedSliceIndex and newFinalizedSliceIndex.
        // at this point we are sure that we will never use them again.
        for (auto si = lastFinalizedSliceIndex; si < newFinalizedSliceIndex; si++) {
            assert(slices.contains(si));
            slices.erase(si);
        }
    }

    /**
     * @brief Returns a list of slices, which can be used to access a single slice.
     * @param startTs window start
     * @param endTs window end
     * @return std::vector<KeyedSliceSharedPtr>
     */
    std::vector<SliceTypeSharedPtr> getSlicesForWindow(uint64_t startTs, uint64_t endTs) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        auto slicesInWindow = std::vector<SliceTypeSharedPtr>();
        // Iterate over the slices and collect all slices, which are covered by the window between startTs and endTs.
        auto sliceIter = slices.begin();
        while (sliceIter != slices.end()) {
            if ((*sliceIter)->getEnd() > endTs) {
                break;
            }
            if ((*sliceIter)->getStart() >= startTs && (*sliceIter)->getStart() <= endTs) {
                slicesInWindow.emplace_back(*sliceIter);
            }
            sliceIter++;
        }
        return slicesInWindow;
    }

  private:
    std::vector<Window> triggerInflightWindows(uint64_t windowSize, uint64_t windowSlide, uint64_t lastEndTs, uint64_t endEndTs) {
        std::vector<Window> windows;
        // trigger all windows, for which the list of slices contains the slice end.
        // use __builtin_sub_overflow to detect negative overflowing sub
        uint64_t maxWindowEndTs = lastEndTs;
        if (__builtin_sub_overflow(maxWindowEndTs, windowSize, &maxWindowEndTs)) {
            maxWindowEndTs = 0;
        }
        // iterate over all slices that would open a window that ends below endTs.
        for (auto& slice : slices) {
            if (slice->getStart() + windowSize > endEndTs) {
                break;
            }
            if (slice->getStart() >= maxWindowEndTs && slice->getEnd() <= endEndTs) {
                auto windowStart = slice->getStart();
                auto windowEnd = slice->getStart() + windowSize;
                // check if it is a valid window
                if ((lastWindowStart == UINT64_MAX || lastWindowStart < windowStart) && ((windowStart % windowSlide) == 0)) {
                    windows.emplace_back<Window>({windowStart, windowEnd, ++emittedWindows});
                    lastWindowStart = windowStart;
                }
            }
        }
        return windows;
    }
    std::mutex sliceStagingMutex;
    std::list<SliceTypeSharedPtr> slices;
    WatermarkProcessor sliceAddSequenceLog;
    WatermarkProcessor sliceTriggerSequenceLog;
    uint64_t lastWindowStart = UINT64_MAX;
    uint64_t emittedWindows = 0;
};

}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_GLOBALSLICESTORE_HPP_
