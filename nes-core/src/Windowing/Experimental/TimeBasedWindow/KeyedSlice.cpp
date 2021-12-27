#include <Windowing/Experimental/HashMap.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedSlice.hpp>
namespace NES::Windowing::Experimental {

KeyedSlice::KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory,
                       uint64_t start,
                       uint64_t end,
                       uint64_t index)
    : start(start), end(end), index(index), state(hashMapFactory->create()) {}
KeyedSlice::KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory) : KeyedSlice(hashMapFactory, 0, 0, 0) {}

void KeyedSlice::reset(uint64_t start, uint64_t end, uint64_t index) {
    this->start = start;
    this->end = end;
    this->index = index;
    this->state.clear();
}
}// namespace NES::Windowing::Experimental