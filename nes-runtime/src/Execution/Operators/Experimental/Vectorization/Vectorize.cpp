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

#include <Execution/Operators/Experimental/Vectorization/Vectorize.hpp>

#include <Execution/RecordBuffer.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>
#include <Execution/Operators/Experimental/Vectorization/StagingHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

bool isStageBufferFull(void* state) {
    auto handler = static_cast<StagingHandler*>(state);
    return handler->full();
}

void resetStageBuffer(void* state) {
    auto handler = static_cast<StagingHandler*>(state);
    handler->reset();
}

void addRecordToStageBuffer(void* state, void* recordPtr) {
    auto handler = static_cast<StagingHandler*>(state);
    auto recordRef = Value<MemRef>((int8_t*)recordPtr);
    auto record = static_cast<Record*>(recordRef->getValue());
    handler->addRecord(*record);
}

void* getStageBuffer(void* state) {
    auto handler = static_cast<StagingHandler*>(state);
    auto bufferRef = handler->getTupleBufferReference();
    return bufferRef->getValue();
}

Vectorize::Vectorize(uint64_t operatorHandlerIndex)
    : operatorHandlerIndex(operatorHandlerIndex)
{

}

void Vectorize::execute(ExecutionContext& ctx, Record& record) const {
    if (hasChild()) {
        auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
        auto recordRef = Value<MemRef>((int8_t*)&record);
        Nautilus::FunctionCall("addRecordToStageBuffer", addRecordToStageBuffer, globalOperatorHandler, recordRef);
        auto stageBufferFull = Nautilus::FunctionCall("isStageBufferFull", isStageBufferFull, globalOperatorHandler);
        if (stageBufferFull) {
            auto tupleBufferRef = Nautilus::FunctionCall("getStageBuffer", getStageBuffer, globalOperatorHandler);
            auto recordBuffer = RecordBuffer(tupleBufferRef);
            auto vectorizedChild = std::dynamic_pointer_cast<VectorizableOperator>(child);
            vectorizedChild->execute(ctx, recordBuffer);
            Nautilus::FunctionCall("resetStageBuffer", resetStageBuffer, globalOperatorHandler);
        }
    }
}

} // namespace NES::Runtime::Execution::Operators