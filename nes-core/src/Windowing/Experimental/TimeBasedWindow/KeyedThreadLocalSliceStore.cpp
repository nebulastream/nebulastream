#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStore.hpp>

namespace NES::Windowing::Experimental {
KeyedThreadLocalSliceStore::KeyedThreadLocalSliceStore(NES::Experimental::HashMapFactoryPtr hashMapFactory,
                                                       uint64_t sliceSize,
                                                       uint64_t numberOfPreallocatedSlices)
    : hashMapFactory(hashMapFactory), sliceSize(sliceSize) {
    for (uint64_t i = 0; i < numberOfPreallocatedSlices; i++) {
        preallocatedSlices.emplace_back(std::make_unique<KeyedSlice>(hashMapFactory, 0, 0, 0));
    }
};

KeyedSlicePtr KeyedThreadLocalSliceStore::allocateNewSlice(uint64_t startTs, uint64_t endTs, uint64_t sliceIndex) {
    //if (!preallocatedSlices.empty()) {
    //    auto slice = std::move(preallocatedSlices.back());
    //    preallocatedSlices.pop_back();
    //    slice->reset(startTs, endTs, sliceIndex);
    //    return slice;
    //} else {
        return std::make_unique<KeyedSlice>(hashMapFactory, startTs, endTs, sliceIndex);
    //}
}

void KeyedThreadLocalSliceStore::dropFirstSlice() {
    // if the first index reaches the last index, we create a dummy slice at the end.
    if (slices.contains(firstIndex)) {
       // auto slice = std::move(slices[firstIndex]);
       // preallocatedSlices.emplace_back(std::move(slice));
        slices.erase(firstIndex);
    }
    this->firstIndex++;
}

uint64_t KeyedThreadLocalSliceStore::getLastWatermark() {
    return lastWatermarkTs;
}

void KeyedThreadLocalSliceStore::setLastWatermark(uint64_t watermarkTs) {
    lastWatermarkTs = watermarkTs;
}

KeyedSlicePtr& KeyedThreadLocalSliceStore::insertSlice(uint64_t sliceIndex) {
    // slices are always pre-initialized. Thus, we can just call reset.
    auto startTs = sliceIndex * sliceSize;
    auto endTs = (sliceIndex + 1) * sliceSize;
    auto slice = allocateNewSlice(startTs, endTs, sliceIndex);
    if (sliceIndex >= lastIndex) {
        if (currentSlice != nullptr) {
            slices[currentSlice->getIndex()] = std::move(currentSlice);
        }
        lastIndex = sliceIndex;
        currentSlice = std::move(slice);
        return currentSlice;
    } else {
        slices[sliceIndex] = std::move(slice);
        return slices[sliceIndex];
    }
}
void KeyedThreadLocalSliceStore::setFirstSliceIndex(uint64_t i) {
    firstIndex = i;
}

}// namespace NES::Windowing::Experimental