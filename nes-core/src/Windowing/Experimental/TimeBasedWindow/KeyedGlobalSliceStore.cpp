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

#include "Windowing/Experimental/TimeBasedWindow/KeyedGlobalSliceStore.hpp"
#include "Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp"

namespace NES::Windowing::Experimental {

std::tuple<uint64_t, uint64_t>
KeyedGlobalSliceStore::addSlice(uint64_t sequenceNumber, uint64_t sliceIndex, KeyedSliceSharedPtr slice) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    //NES_ASSERT(!sliceMap.contains(sliceIndex), "Slice is not contained");
    auto sliceIter = slices.rbegin();
    while (sliceIter != slices.rend() && (*sliceIter)->getStart() > slice->getStart()) {
        sliceIter++;
    }
    if (sliceIter == slices.rend()) {
        // We are in case 1. thus we have to prepend a new slice
        slices.emplace_front(std::move(slice));
    } else if ((*sliceIter)->getStart() < slice->getStart()) {
        slices.emplace(sliceIter.base(), std::move(slice));
    } else {
        throw WindowProcessingException("");
    }
    auto lastMaxSliceIndex = sliceAddSequenceLog.getCurrentWatermark();
    sliceAddSequenceLog.updateWatermark(sliceIndex, sequenceNumber);
    auto newMaxSliceIndex = sliceAddSequenceLog.getCurrentWatermark();
    return {lastMaxSliceIndex, newMaxSliceIndex};
}

KeyedSliceSharedPtr KeyedGlobalSliceStore::getSlice(uint64_t sliceEnd) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    //NES_ASSERT(sliceMap.contains(sliceIndex), "slice is not contained");
    //return sliceMap[sliceIndex];
    auto sliceIter = slices.begin();
    while (sliceIter != slices.end() && (*sliceIter)->getEnd() < sliceEnd) {

        if ((*sliceIter)->getEnd() == sliceEnd) {
            return (*sliceIter);
        }
        sliceIter++;
    }
    throw WindowProcessingException("slice dose not exists");
}

KeyedGlobalSliceStore::~KeyedGlobalSliceStore(){
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
    if (__builtin_sub_overflow(sliceIndex, slicesPerWindow, &finalizeSliceIndex)) {
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
bool KeyedGlobalSliceStore::hasSlice(uint64_t) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    //  return sliceMap.contains(sliceIndex);
    return true;
}

void KeyedGlobalSliceStore::clear() {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    //sliceMap.clear();
}

std::vector<KeyedSliceSharedPtr> KeyedGlobalSliceStore::getSlicesForWindow(uint64_t startTs, uint64_t endTs) {
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