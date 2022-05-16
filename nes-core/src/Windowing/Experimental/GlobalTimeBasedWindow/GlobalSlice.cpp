#include <Windowing/Experimental/GlobalTimeBasedWindow/GlobalSlice.hpp>
#include <stdio.h>
#include <string.h>

namespace NES::Windowing::Experimental {

uint32_t alignBufferSize(uint32_t bufferSize, uint32_t withAlignment) {
    if (bufferSize % withAlignment) {
        // make sure that each buffer is a multiple of the alignment
        return bufferSize + (withAlignment - bufferSize % withAlignment);
    }
    return bufferSize;
}

void State::reset() { isInitialized = false; }

State::State(uint64_t stateSize)
    : stateSize(stateSize), ptr(std::aligned_alloc(STATE_ALIGNMENT, alignBufferSize(stateSize, STATE_ALIGNMENT))){};

State::~State() { free(ptr); }

GlobalSlice::GlobalSlice(uint64_t entrySize, uint64_t start, uint64_t end)
    : start(start), end(end), state(std::make_unique<State>(entrySize)) {}

GlobalSlice::GlobalSlice(uint64_t entrySize) : start(0), end(0), state(std::make_unique<State>(entrySize)) {}

void GlobalSlice::reset(uint64_t start, uint64_t end) {
    this->start = start;
    this->end = end;
    this->state->reset();
}

GlobalSlice::GlobalSlice(GlobalSlice& entrySize) : start(0), end(0), state(std::make_unique<State>(entrySize.state->stateSize)) {}

}// namespace NES::Windowing::Experimental