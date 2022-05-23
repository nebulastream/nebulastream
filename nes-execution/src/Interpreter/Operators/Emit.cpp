
#include <Interpreter/ExecutionContext.hpp>
#include <Interpreter/Operators/Emit.hpp>
#include <Interpreter/Operators/ExecuteOperator.hpp>
#include <Interpreter/Record.hpp>
#include <Interpreter/RecordBuffer.hpp>
namespace NES::Interpreter {

class EmitState : public OperatorState {
  public:
    EmitState(RecordBuffer resultBuffer) : resultBuffer(resultBuffer) {}
    Value<Integer> outputIndex = 0;
    RecordBuffer resultBuffer;
};

void Emit::open(ExecutionContext& ctx, RecordBuffer&) const {
    // initialize state variable and create new buffer
    auto resultBufferRef = ctx.allocateBuffer();
    auto resultBuffer = RecordBuffer(resultMemoryLayout, resultBufferRef);
    ctx.setLocalOperatorState(this, std::make_unique<EmitState>(resultBuffer));
}

void Emit::execute(ExecutionContext& ctx, Record& recordBuffer) const {
    auto emitState = (EmitState*) ctx.getLocalState(this);
    auto resultBuffer = emitState->resultBuffer;
    auto outputIndex = emitState->outputIndex;
    resultBuffer.write(outputIndex, recordBuffer);
    emitState->outputIndex = outputIndex + 1;
    // emit buffer if it reached the maximal capacity
    if (outputIndex >= maxRecordsPerBuffer) {
        resultBuffer.setNumRecords(emitState->outputIndex);
        ctx.emitBuffer(resultBuffer);
        auto resultBufferRef = ctx.allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultMemoryLayout, resultBufferRef);
        emitState->outputIndex = 0;
    }
}

void Emit::close(ExecutionContext& ctx, RecordBuffer&) const {
    // emit current buffer and set the number of records
    auto emitState = (EmitState*) ctx.getLocalState(this);
    auto resultBuffer = emitState->resultBuffer;
    resultBuffer.setNumRecords(emitState->outputIndex);
    ctx.emitBuffer(resultBuffer);
}

Emit::Emit(Runtime::MemoryLayouts::MemoryLayoutPtr resultMemoryLayout)
    : maxRecordsPerBuffer(resultMemoryLayout->getCapacity()), resultMemoryLayout(resultMemoryLayout) {}

}// namespace NES::Interpreter