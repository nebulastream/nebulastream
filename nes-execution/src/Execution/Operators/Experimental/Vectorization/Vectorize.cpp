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
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution::Operators {

void* getTupleBuffer(void* state) {
    auto handler = static_cast<StagingHandler*>(state);
    return handler->getTupleBuffer();
}

void* getStageBuffer(void* state) {
    auto tupleBufferRef = getTupleBuffer(state);
    auto tupleBuffer = static_cast<TupleBuffer*>(tupleBufferRef);
    return tupleBuffer->getBuffer();
}

uint64_t getCurrentWriteIndexAndIncrement(void* state) {
    auto handler = static_cast<StagingHandler*>(state);
    return handler->getCurrentWritePositionAndIncrement();
}

bool isStageBufferFull(void* state) {
    auto handler = static_cast<StagingHandler*>(state);
    return handler->full();
}

void resetStageBuffer(void* state) {
    auto handler = static_cast<StagingHandler*>(state);
    handler->reset();
}

Vectorize::Vectorize(uint64_t operatorHandlerIndex, std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider)
    : operatorHandlerIndex(operatorHandlerIndex)
    , memoryProvider(std::move(memoryProvider))
{

}

void Vectorize::execute(ExecutionContext& ctx, Record& record) const {
    if (hasChild()) {
        auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
        auto stageBufferAddress = Nautilus::FunctionCall("getStageBuffer", getStageBuffer, globalOperatorHandler);
        auto writeIndex = Nautilus::FunctionCall("getCurrentWriteIndexAndIncrement", getCurrentWriteIndexAndIncrement, globalOperatorHandler);
        memoryProvider->write(writeIndex, stageBufferAddress, record);
        auto stageBufferFull = Nautilus::FunctionCall("isStageBufferFull", isStageBufferFull, globalOperatorHandler);
        if (stageBufferFull) {
            auto tupleBufferRef = Nautilus::FunctionCall("getTupleBuffer", getTupleBuffer, globalOperatorHandler);
            auto recordBuffer = RecordBuffer(tupleBufferRef);
            auto vectorizedChild = std::dynamic_pointer_cast<const VectorizableOperator>(child);
            vectorizedChild->execute(ctx, recordBuffer);
            Nautilus::FunctionCall("resetStageBuffer", resetStageBuffer, globalOperatorHandler);
        }
    }
}

} // namespace NES::Runtime::Execution::Operators