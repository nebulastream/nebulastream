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
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
namespace NES::Runtime::Execution::Operators {

uint32_t alignBufferSize(uint32_t bufferSize, uint32_t withAlignment) {
    if (bufferSize % withAlignment) {
        // make sure that each buffer is a multiple of the alignment
        return bufferSize + (withAlignment - bufferSize % withAlignment);
    }
    return bufferSize;
}

State::State(uint64_t stateSize)
    : stateSize(stateSize), ptr(std::aligned_alloc(STATE_ALIGNMENT, alignBufferSize(stateSize, STATE_ALIGNMENT))){};

State::~State() { free(ptr); }

NonKeyedSlice::NonKeyedSlice(uint64_t entrySize, uint64_t start, uint64_t end, const std::unique_ptr<State>& defaultState)
    : start(start), end(end), state(std::make_unique<State>(entrySize)) {
    std::memcpy(state->ptr, defaultState->ptr, entrySize);
}
NonKeyedSlice::~NonKeyedSlice() {
    NES_DEBUG("~NonKeyedSlice {}-{}", start, end);
}

}// namespace NES::Runtime::Execution::Operators