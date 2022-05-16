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

#include "Windowing/Experimental/KeyedTimeWindow/KeyedGlobalSliceStore.hpp"
#include "Windowing/Experimental/KeyedTimeWindow/KeyedSlice.hpp"
#include <forward_list>
namespace NES::Windowing::Experimental {

std::vector<Window> KeyedGlobalSliceStore::addSliceAndTriggerWindows(uint64_t sequenceNumber,
                                                                     KeyedSliceSharedPtr slice,
                                                                     uint64_t windowSize,
                                                                     uint64_t windowSlide) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);

    //NES_ASSERT(!sliceMap.contains(sliceIndex), "Slice is not contained");
    auto sliceEnd = slice->getEnd();
    auto sliceStart = slice->getStart();
    auto sliceIter = slices.rbegin();
    while (sliceIter != slices.rend() && (*sliceIter)->getStart() > sliceStart) {
        sliceIter++;
    }
    if (sliceIter == slices.rend()) {
        // We are in case 1. thus we have to prepend a new slice
        slices.emplace_front(std::move(slice));
    } else if ((*sliceIter)->getStart() < sliceStart) {
        slices.emplace(sliceIter.base(), std::move(slice));
    } else {
        throw WindowProcessingException("");
    }
    auto lastMaxSliceEnd = sliceAddSequenceLog.getCurrentWatermark();
    sliceAddSequenceLog.updateWatermark(sliceEnd, sequenceNumber);
    auto newMaxSliceEnd = sliceAddSequenceLog.getCurrentWatermark();
    return triggerInflightWindows(windowSize, windowSlide, lastMaxSliceEnd, newMaxSliceEnd);
}

std::vector<Window>
KeyedGlobalSliceStore::triggerInflightWindows(uint64_t windowSize, uint64_t windowSlide, uint64_t lastEndTs, uint64_t endEndTs) {

    std::vector<Window> windows;
    // trigger all windows, for which the list of slices contains the slice end.
    uint64_t maxWindowEndTs = lastEndTs;
    if (__builtin_sub_overflow(maxWindowEndTs, windowSize, &maxWindowEndTs)) {
        maxWindowEndTs = 0;
    }

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

std::vector<Window> KeyedGlobalSliceStore::triggerAllInflightWindows(uint64_t windowSize, uint64_t windowSlide) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    auto lastMaxSliceEnd = sliceAddSequenceLog.getCurrentWatermark();
    return triggerInflightWindows(windowSize, windowSlide, lastMaxSliceEnd, slices.back()->getEnd() + windowSize);
}

KeyedGlobalSliceStore::~KeyedGlobalSliceStore() {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    NES_DEBUG("~KeyedGlobalSliceStore")
    slices.clear();
}

void KeyedGlobalSliceStore::finalizeSlice(uint64_t sequenceNumber, uint64_t sliceIndex) {
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
        //assert(sliceMap.contains(si));
        // sliceMap.erase(si);
    }
}

std::vector<KeyedSliceSharedPtr> KeyedGlobalSliceStore::getSlicesForWindow(uint64_t startTs, uint64_t endTs) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    auto slicesInWindow = std::vector<KeyedSliceSharedPtr>();
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

}// namespace NES::Windowing::Experimental