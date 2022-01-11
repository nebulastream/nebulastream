#include <Windowing/Experimental/TimeBasedWindow/KeyedGlobalSliceStore.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>

namespace NES::Windowing::Experimental {

std::tuple<uint64_t, uint64_t> KeyedGlobalSliceStore::addSlice(uint64_t sequenceNumber, uint64_t sliceIndex, KeyedSlicePtr slice) {
    const std::lock_guard<std::mutex> lock(sliceStagingMutex);
    assert(!sliceMap.contains(sliceIndex));
    sliceMap[sliceIndex] = slice;
    auto lastMaxSliceIndex = sliceAddSequenceLog.getCurrentWatermark();
    sliceAddSequenceLog.updateWatermark(sequenceNumber, sliceIndex);
    auto newMaxSliceIndex = sliceAddSequenceLog.getCurrentWatermark();
    return { lastMaxSliceIndex, newMaxSliceIndex };
}

}// namespace NES::Windowing::Experimental