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

#include <EmitPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/val.hpp>
#include <EmitOperatorHandler.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <function.hpp>
#include <val_ptr.hpp>

namespace NES
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
nautilus::val<bool> isLastChunk(ExecutionContext& context, OperatorHandlerId operatorHandlerId)
{
    return nautilus::invoke(
        isLastChunkProxy,
        context.getGlobalOperatorHandler(operatorHandlerId),
        context.originId,
        context.sequenceNumber,
        context.chunkNumber,
        context.lastChunk);
}

nautilus::val<ChunkNumber> getNextChunkNr(ExecutionContext& context, OperatorHandlerId operatorHandlerId)
{
    return nautilus::invoke(
        getNextChunkNumberProxy, context.getGlobalOperatorHandler(operatorHandlerId), context.originId, context.sequenceNumber);
}

void removeSequenceState(ExecutionContext& context, OperatorHandlerId operatorHandlerId)
{
    nautilus::invoke(
        removeSequenceStateProxy, context.getGlobalOperatorHandler(operatorHandlerId), context.originId, context.sequenceNumber);
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

void EmitPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer&) const
{
    /// initialize state variable and create new buffer
    const auto resultBufferRef = ctx.allocateBuffer();
    const auto resultBuffer = RecordBuffer(resultBufferRef);
    auto emitState = std::make_unique<EmitState>(resultBuffer);
    ctx.setLocalOperatorState(id, std::move(emitState));
}

void EmitPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(id));
    /// emit buffer if it reached the maximal capacity
    if (emitState->outputIndex >= getMaxRecordsPerBuffer())
    {
        emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, false);
        const auto resultBufferRef = ctx.allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultBufferRef);
        emitState->bufferMemoryArea = emitState->resultBuffer.getBuffer();
        emitState->outputIndex = 0_u64;
    }

    /// We need to first check if the buffer has to be emitted and then write to it. Otherwise, it can happen that we will
    /// emit a tuple twice. Once in the execute() and then again in close(). This happens only for buffers that are filled
    /// to the brim, i.e., have no more space left.
    memoryProvider->writeRecord(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    emitState->outputIndex = emitState->outputIndex + 1;
}

void EmitPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    /// emit current buffer and set the metadata
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(id));
    emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, true);
}

void EmitPhysicalOperator::emitRecordBuffer(
    ExecutionContext& ctx,
    RecordBuffer& recordBuffer,
    const nautilus::val<uint64_t>& numRecords,
    const nautilus::val<bool>& potentialLastChunk) const
{
    const auto tupleSize = memoryProvider->getMemoryLayout()->getSchema().getSizeOfSchemaInBytes();
    const auto usedMemorySize = numRecords * tupleSize;

    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.watermarkTs);
    recordBuffer.setOriginId(ctx.originId);
    recordBuffer.setSequenceNumber(ctx.sequenceNumber);
    recordBuffer.setCreationTs(ctx.currentTs);
    recordBuffer.setUsedMemorySize(usedMemorySize);

    /// Chunk Logic. Order matters.
    /// A worker thread will clean up the sequence state for the current sequence number if its told it is the last
    /// chunk number. Thus it is important to query the state first to get the next chunk number, before asking for the lastChunk
    /// as this will mark the current chunknumber as processed and my allow a different worker thread to concurrently clean up.
    recordBuffer.setChunkNumber(getNextChunkNr(ctx, operatorHandlerId));
    if (potentialLastChunk && isLastChunk(ctx, operatorHandlerId))
    {
        removeSequenceState(ctx, operatorHandlerId);
        recordBuffer.setLastChunk(true);
    }
    else
    {
        recordBuffer.setLastChunk(false);
    }

    ctx.emitBuffer(recordBuffer);
}

EmitPhysicalOperator::EmitPhysicalOperator(
    OperatorHandlerId operatorHandlerId, std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : memoryProvider(std::move(memoryProvider)), operatorHandlerId(operatorHandlerId)
{
}

[[nodiscard]] uint64_t EmitPhysicalOperator::getMaxRecordsPerBuffer() const
{
    return memoryProvider->getMemoryLayout()->getCapacity();
}

std::optional<PhysicalOperator> EmitPhysicalOperator::getChild() const
{
    return child;
}

void EmitPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
