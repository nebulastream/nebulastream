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

#include <Execution/Operators/Experimental/Vectorization/StagingHandler.hpp>

namespace NES::Runtime::Execution::Operators {

StagingHandler::StagingHandler(std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider, const Value<MemRef>& tupleBufferAddress, uint64_t stageBufferCapacity)
    : memoryProvider(std::move(memoryProvider))
    , tupleBufferAddress(tupleBufferAddress)
    , stageBufferCapacity(stageBufferCapacity)
    , recordBuffer(tupleBufferAddress)
    , currentWritePosition((uint64_t) 0)
{

}

void StagingHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
}

void StagingHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {
}

void StagingHandler::addRecord(Record& record) {
    auto writeAddress = recordBuffer.getBuffer();
    memoryProvider->write(currentWritePosition, writeAddress, record);
    currentWritePosition = currentWritePosition + (uint64_t) 1;
    recordBuffer.setNumRecords(currentWritePosition);
}

void StagingHandler::reset() {
    currentWritePosition = (uint64_t) 0;
}

bool StagingHandler::full() const {
    return currentWritePosition >= stageBufferCapacity;
}

const Value<MemRef>& StagingHandler::getTupleBufferReference() const {
    return tupleBufferAddress;
}

} // namespace NES::Runtime::Execution::Operators
