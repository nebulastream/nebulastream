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

#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Emit.hpp>
#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Experimental/Interpreter/RecordBuffer.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

class EmitState : public OperatorState {
  public:
    EmitState(RecordBuffer resultBuffer) : resultBuffer(resultBuffer) {}
    Value<UInt64> outputIndex = 0ul;
    RecordBuffer resultBuffer;
};

void Emit::open(RuntimeExecutionContext& ctx, RecordBuffer&) const {
    // initialize state variable and create new buffer
    auto resultBufferRef = ctx.getWorkerContext().allocateBuffer();
    auto resultBuffer = RecordBuffer(resultBufferRef);
    ctx.setLocalOperatorState(this, std::make_unique<EmitState>(resultBuffer));
}

void Emit::execute(RuntimeExecutionContext& ctx, Record& recordBuffer) const {
    auto emitState = (EmitState*) ctx.getLocalState(this);
    auto resultBuffer = emitState->resultBuffer;
    auto outputIndex = emitState->outputIndex;
    resultBuffer.write(resultMemoryLayout, outputIndex, recordBuffer);
    emitState->outputIndex = outputIndex + 1ul;
    // emit buffer if it reached the maximal capacity
   /* if (outputIndex >= maxRecordsPerBuffer) {
        resultBuffer.setNumRecords(emitState->outputIndex);
        ctx.getPipelineContext().emitBuffer(ctx.getWorkerContext(), resultBuffer);
        auto resultBufferRef = ctx.getWorkerContext().allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultBufferRef);
        emitState->outputIndex = 0ul;
    }*/
}

void Emit::close(RuntimeExecutionContext& ctx, RecordBuffer&) const {
    // emit current buffer and set the number of records
    auto emitState = (EmitState*) ctx.getLocalState(this);
    auto resultBuffer = emitState->resultBuffer;
    resultBuffer.setNumRecords(emitState->outputIndex);
    ctx.getPipelineContext().emitBuffer(ctx.getWorkerContext(), resultBuffer);
}

Emit::Emit(Runtime::MemoryLayouts::MemoryLayoutPtr resultMemoryLayout)
    : maxRecordsPerBuffer(resultMemoryLayout->getCapacity()), resultMemoryLayout(resultMemoryLayout) {}

}// namespace NES::ExecutionEngine::Experimental::Interpreter