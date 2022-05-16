#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlice.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalThreadLocalSliceStore.hpp>
#include <memory>

namespace NES::Windowing::Experimental {

GlobalThreadLocalSliceStore::GlobalThreadLocalSliceStore(uint64_t entrySize, uint64_t windowSize, uint64_t windowSlide)
    : ThreadLocalSliceStore(windowSize, windowSlide), entrySize(entrySize) {}

GlobalSlicePtr GlobalThreadLocalSliceStore::allocateNewSlice(uint64_t startTs, uint64_t endTs) {
    return std::make_unique<GlobalSlice>(entrySize, startTs, endTs);
}

}// namespace NES::Windowing::Experimental