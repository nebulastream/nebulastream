#include <Windowing/Experimental/TimeBasedWindow/KeyedGlobalSliceStore.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>

namespace NES::Windowing::Experimental {

std::tuple<uint64_t, uint64_t>
KeyedGlobalSliceStore::addSlice(uint64_t sequenceNumber, uint64_t sliceIndex, KeyedSliceSharedPtr slice) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    assert(!sliceMap.contains(sliceIndex));
    sliceMap[sliceIndex] = slice;
    auto lastMaxSliceIndex = sliceAddSequenceLog.getCurrentWatermark();
    sliceAddSequenceLog.updateWatermark(sliceIndex, sequenceNumber);
    auto newMaxSliceIndex = sliceAddSequenceLog.getCurrentWatermark();
    return {lastMaxSliceIndex, newMaxSliceIndex};
}

KeyedSliceSharedPtr KeyedGlobalSliceStore::getSlice(uint64_t sliceIndex) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    assert(sliceMap.contains(sliceIndex));
    return sliceMap[sliceIndex];
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
        assert(sliceMap.contains(si));
        sliceMap.erase(si);
    }
}
bool KeyedGlobalSliceStore::hasSlice(uint64_t sliceIndex) {
    return sliceMap.contains(sliceIndex);
}

}// namespace NES::Windowing::Experimental