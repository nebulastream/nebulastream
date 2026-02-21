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

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
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

class EmitState : public OperatorState
{
public:
    explicit EmitState(const RecordBuffer& resultBuffer) : resultBuffer(resultBuffer), bufferMemoryArea(resultBuffer.getMemArea()) { }

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

    /// We need to first check if the buffer has to be emitted and then write to it. Otherwise, it can happen that we will
    /// emit a tuple twice. Once in the execute() and then again in close(). This happens only for buffers that are filled
    /// to the brim, i.e., have no more space left.
    nautilus::val<uint64_t> recordsWritten = 0;
    recordsWritten
        = bufferRef->writeRecordSafely(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    /// recordsWritten == npos indicates that the buffer must be emitted first. We emit and than call write record again.
    if (recordsWritten == nautilus::val<uint64_t>(std::numeric_limits<uint64_t>::max()))
    {
        emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, false);
        const auto resultBufferRef = ctx.allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultBufferRef);
        emitState->bufferMemoryArea = emitState->resultBuffer.getMemArea();
        emitState->outputIndex = 0_u64;

        recordsWritten
            = bufferRef->writeRecord(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    }

    /// If our BufferRef is an OutputFormatterBufferRef, records written contains the amount of bytes that were written and not the amount of records,
    /// since this is the only way to retrieve the amount of bytes in the buffer in this case.
    emitState->outputIndex = emitState->outputIndex + recordsWritten;
}

void EmitPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    /// emit current buffer and set the metadata
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(id));
    emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, true);
}

namespace
{
void setChunkNumber(
    const ExecutionContext& context,
    OperatorHandlerId operatorHandlerId,
    const nautilus::val<bool>& closesChunk,
    const nautilus::val<ChunkNumber>& currentChunkNumber,
    const nautilus::val<bool>& isCurrentBufferTheLastChunk,
    const nautilus::val<TupleBuffer*>& newBuffer)
{
    nautilus::invoke(
        +[](OperatorHandler* handler,
            bool closesChunk,
            ChunkNumber currentChunkNumber,
            bool isCurrentBufferTheLastChunk,
            TupleBuffer* newBuffer)
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(newBuffer != nullptr, "Expects a valid buffer");
            PRECONDITION(currentChunkNumber != INVALID<ChunkNumber>, "Expects a valid chunkNumber");

            dynamic_cast<EmitOperatorHandler&>(*handler).setChunkNumber(
                closesChunk, currentChunkNumber, isCurrentBufferTheLastChunk, *newBuffer);
        },
        context.getGlobalOperatorHandler(operatorHandlerId),
        closesChunk,
        currentChunkNumber,
        isCurrentBufferTheLastChunk,
        newBuffer);
}
}

void EmitPhysicalOperator::emitRecordBuffer(
    ExecutionContext& ctx,
    RecordBuffer& recordBuffer,
    const nautilus::val<uint64_t>& numRecords,
    const nautilus::val<bool>& potentialLastChunk) const
{
    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.watermarkTs);
    recordBuffer.setOriginId(ctx.originId);
    recordBuffer.setSequenceNumber(ctx.sequenceNumber);
    recordBuffer.setCreationTs(ctx.currentTs);

    setChunkNumber(ctx, operatorHandlerId, potentialLastChunk, ctx.chunkNumber, ctx.lastChunk, recordBuffer.getReference());

    ctx.emitBuffer(recordBuffer);
}

EmitPhysicalOperator::EmitPhysicalOperator(OperatorHandlerId operatorHandlerId, std::shared_ptr<TupleBufferRef> memoryProvider)
    : bufferRef(std::move(memoryProvider)), operatorHandlerId(operatorHandlerId)
{
}

[[nodiscard]] uint64_t EmitPhysicalOperator::getMaxRecordsPerBuffer() const
{
    return bufferRef->getCapacity();
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
