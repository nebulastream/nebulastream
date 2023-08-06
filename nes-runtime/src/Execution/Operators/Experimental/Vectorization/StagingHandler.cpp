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

#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution::Operators {

StagingHandler::StagingHandler(std::unique_ptr<TupleBuffer> tupleBuffer, uint64_t stageBufferCapacity)
    : tupleBuffer(std::move(tupleBuffer))
    , stageBufferCapacity(stageBufferCapacity)
    , currentWritePosition(0)
{

}

void StagingHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
}

void StagingHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {
}

void StagingHandler::reset() {
    currentWritePosition = 0;
}

bool StagingHandler::full() const {
    return currentWritePosition >= stageBufferCapacity;
}

TupleBuffer* StagingHandler::getTupleBuffer() const {
    return tupleBuffer.get();
}

uint64_t StagingHandler::getCurrentWritePosition() const {
    return currentWritePosition;
}

void StagingHandler::incrementWritePosition() {
    // TODO Modification to the write position depends on the schema.
    currentWritePosition = currentWritePosition + 1;
    tupleBuffer->setNumberOfTuples(currentWritePosition);
}

} // namespace NES::Runtime::Execution::Operators
