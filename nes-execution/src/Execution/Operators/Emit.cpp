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

#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators {

class EmitState : public OperatorState {
  public:
    explicit EmitState(const RecordBuffer& resultBuffer)
        : resultBuffer(resultBuffer), bufferReference(resultBuffer.getBuffer()) {}
    nautilus::val<uint64_t> outputIndex = 0_u64;
    RecordBuffer resultBuffer;
    nautilus::val<int8_t*> bufferReference;
};

void Emit::open(ExecutionContext& ctx, RecordBuffer&) const {
    // initialize state variable and create new buffer
    auto resultBufferRef = ctx.allocateBuffer();
    auto resultBuffer = RecordBuffer(resultBufferRef);
    ctx.setLocalOperatorState(this, std::make_unique<EmitState>(resultBuffer));
}

void Emit::execute(ExecutionContext& ctx, Record& record) const {
    auto emitState = (EmitState*) ctx.getLocalState(this);
    // emit buffer if it reached the maximal capacity
    const auto result = emitState->outputIndex >= nautilus::val<uint64_t>(maxRecordsPerBuffer);
    if (result.value) {
        emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, false);
        auto resultBufferRef = ctx.allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultBufferRef);
        emitState->bufferReference = emitState->resultBuffer.getBuffer();
        emitState->outputIndex = 0_u64;
    }

    /* We need to first check if the buffer has to be emitted and then write to it. Otherwise, it can happen that we will
     * emit a tuple twice. Once in the execute() and then again in close(). This happens only for buffers that are filled
     * to the brim, i.e., have no more space left.
     */
    memoryProvider->write(emitState->outputIndex, emitState->bufferReference, record);
    emitState->outputIndex = emitState->outputIndex + 1_u64;
}

void Emit::close(ExecutionContext& ctx, RecordBuffer&) const {
    // emit current buffer and set the metadata
    auto emitState = (EmitState*) ctx.getLocalState(this);
    emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, ctx.isLastChunk());
}

void Emit::emitRecordBuffer(ExecutionContext& ctx,
                            RecordBuffer& recordBuffer,
                            const val<uint64_t>& numRecords,
                            const val<bool>& lastChunk) const {
    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.getWatermarkTs());
    recordBuffer.setOriginId(ctx.getOriginId());
    recordBuffer.setStatisticId(ctx.getCurrentStatisticId());
    recordBuffer.setSequenceNr(ctx.getSequenceNumber());
    recordBuffer.setChunkNr(ctx.getNextChunkNr());
    recordBuffer.setLastChunk(lastChunk);
    recordBuffer.setCreationTs(ctx.getCurrentTs());

    NES_INFO("Emitting recordBuffer {}-{}-{} with {} tuples",
             recordBuffer.getSequenceNr().toString(),
             recordBuffer.getChunkNr().toString(),
             recordBuffer.isLastChunk().toString(),
             recordBuffer.getNumRecords().toString());

    ctx.emitBuffer(recordBuffer);

    if (lastChunk == true) {
        ctx.removeSequenceState();
    }
}

Emit::Emit(std::unique_ptr<MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : maxRecordsPerBuffer(memoryProvider->getMemoryLayoutPtr()->getCapacity()), memoryProvider(std::move(memoryProvider)) {}

}// namespace NES::Runtime::Execution::Operators
