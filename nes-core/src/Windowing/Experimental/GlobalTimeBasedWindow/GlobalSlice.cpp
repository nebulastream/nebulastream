#include <Windowing/Experimental/GlobalTimeBasedWindow/GlobalSlice.hpp>
#include <stdio.h>
#include <string.h>

namespace NES::Windowing::Experimental {

void State::reset() { memset(ptr, 0, stateSize); }

State::State(uint64_t stateSize) : stateSize(stateSize), ptr(std::aligned_alloc(stateSize, STATE_ALIGNMENT)){};

State::~State() { free(ptr); }

GlobalSlice::GlobalSlice(uint64_t entrySize, uint64_t start, uint64_t end)
    : start(start), end(end), state(std::make_unique<State>(entrySize)) {}

GlobalSlice::GlobalSlice(uint64_t entrySize) : start(0), end(0), state(std::make_unique<State>(entrySize)) {}

void GlobalSlice::reset(uint64_t start, uint64_t end) {
    this->start = start;
    this->end = end;
    this->state->reset();
}

GlobalSlice::GlobalSlice(GlobalSlice& entrySize): start(0), end(0), state(std::make_unique<State>(entrySize.state->stateSize)) {}

}// namespace NES::Windowing::Experimental