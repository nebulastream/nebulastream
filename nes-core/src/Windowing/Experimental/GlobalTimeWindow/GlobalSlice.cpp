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
#include <Runtime/TupleBuffer.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlice.hpp>

namespace NES::Windowing::Experimental {

void State::reset() { isInitialized = false; }

State::State(uint64_t stateSize)
    : stateSize(stateSize), ptr(std::aligned_alloc(STATE_ALIGNMENT, Runtime::alignBufferSize(stateSize, STATE_ALIGNMENT))){};

State::~State() { std::free(ptr); }

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