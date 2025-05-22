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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/EmitOperatorHandler.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Util.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/val.hpp>
#include <function.hpp>

namespace NES::Runtime::Execution::Operators
{

uint64_t getNextChunkNumberProxy(void* operatorHandlerPtr, OriginId originId, SequenceNumber sequenceNumber)
{
    PRECONDITION(operatorHandlerPtr != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<EmitOperatorHandler*>(operatorHandlerPtr);
    auto chunkNumber = pipelineCtx->getNextChunkNumber({.sequenceNumber = SequenceNumber(sequenceNumber), .originId = OriginId(originId)});
    NES_TRACE("(Sequence Number: {}, Chunk Number: {})", sequenceNumber, chunkNumber);
    return chunkNumber;
}

bool isLastChunkProxy(void* operatorHandlerPtr, OriginId originId, SequenceNumber sequenceNumber, ChunkNumber chunkNumber, bool isLastChunk)
{
    PRECONDITION(operatorHandlerPtr != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<EmitOperatorHandler*>(operatorHandlerPtr);
    return pipelineCtx->processChunkNumber(
        {.sequenceNumber = SequenceNumber(sequenceNumber), .originId = OriginId(originId)}, ChunkNumber(chunkNumber), isLastChunk);
}

void removeSequenceStateProxy(void* operatorHandlerPtr, OriginId originId, SequenceNumber sequenceNumber)
{
    PRECONDITION(operatorHandlerPtr != nullptr, "operator handler should not be null");
    auto* pipelineCtx = static_cast<EmitOperatorHandler*>(operatorHandlerPtr);
    pipelineCtx->removeSequenceState({.sequenceNumber = SequenceNumber(sequenceNumber), .originId = OriginId(originId)});
}

namespace
{
nautilus::val<bool> isLastChunk(ExecutionContext& context, size_t operatorHandlerIndex)
{
    return nautilus::invoke(
        isLastChunkProxy,
        context.getGlobalOperatorHandler(operatorHandlerIndex),
        context.originId,
        context.sequenceNumber,
        context.chunkNumber,
        context.lastChunk);
}

nautilus::val<ChunkNumber> getNextChunkNr(ExecutionContext& context, size_t operatorHandlerIndex)
{
    return nautilus::invoke(
        getNextChunkNumberProxy, context.getGlobalOperatorHandler(operatorHandlerIndex), context.originId, context.sequenceNumber);
}

void removeSequenceState(ExecutionContext& context, size_t operatorHandlerIndex)
{
    nautilus::invoke(
        removeSequenceStateProxy, context.getGlobalOperatorHandler(operatorHandlerIndex), context.originId, context.sequenceNumber);
}
}

class EmitState : public OperatorState
{
public:
    explicit EmitState(const RecordBuffer& resultBuffer) : resultBuffer(resultBuffer), bufferMemoryArea(resultBuffer.getBuffer()) { }

    nautilus::val<uint64_t> outputIndex = 0;
    RecordBuffer resultBuffer;
    nautilus::val<int8_t*> bufferMemoryArea;
};

void Emit::open(ExecutionContext& ctx, RecordBuffer&) const
{
    /// initialize state variable and create new buffer
    const auto resultBufferRef = ctx.allocateBuffer();
    const auto resultBuffer = RecordBuffer(resultBufferRef);
    auto emitState = std::make_unique<EmitState>(resultBuffer);
    ctx.setLocalOperatorState(this, std::move(emitState));
}

void Emit::execute(ExecutionContext& ctx, Record& record) const
{
    const auto emitState = static_cast<EmitState*>(ctx.getLocalState(this));
    /// emit buffer if it reached the maximal capacity
    if (emitState->outputIndex >= maxRecordsPerBuffer)
    {
        emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, false);
        const auto resultBufferRef = ctx.allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultBufferRef);
        emitState->bufferMemoryArea = emitState->resultBuffer.getBuffer();
        emitState->outputIndex = 0_u64;
    }

    /* We need to first check if the buffer has to be emitted and then write to it. Otherwise, it can happen that we will
     * emit a tuple twice. Once in the execute() and then again in close(). This happens only for buffers that are filled
     * to the brim, i.e., have no more space left.
     */
    memoryProvider->writeRecord(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    emitState->outputIndex = emitState->outputIndex + 1;
}

void Emit::close(ExecutionContext& ctx, RecordBuffer&) const
{
    /// emit current buffer and set the metadata
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(this));
    emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, isLastChunk(ctx, operatorHandlerIndex));
}

void Emit::emitRecordBuffer(
    ExecutionContext& ctx,
    RecordBuffer& recordBuffer,
    const nautilus::val<uint64_t>& numRecords,
    const nautilus::val<bool>& isLastChunk) const
{
    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.watermarkTs);
    recordBuffer.setOriginId(ctx.originId);
    recordBuffer.setSequenceNumber(ctx.sequenceNumber);
    recordBuffer.setChunkNumber(getNextChunkNr(ctx, operatorHandlerIndex));
    recordBuffer.setLastChunk(isLastChunk);
    recordBuffer.setCreationTs(ctx.currentTs);
    ctx.emitBuffer(recordBuffer);

    if (isLastChunk == true)
    {
        removeSequenceState(ctx, operatorHandlerIndex);
    }
}

Emit::Emit(size_t operatorHandlerIndex, std::unique_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : operatorHandlerIndex(operatorHandlerIndex)
    , maxRecordsPerBuffer(memoryProvider->getMemoryLayout()->getCapacity())
    , memoryProvider(std::move(memoryProvider))
{
}

}
