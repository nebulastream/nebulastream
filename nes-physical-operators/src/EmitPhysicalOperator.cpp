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
#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <CompilationContext.hpp>
#include <EmitOperatorHandler.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
class EmitState final : public OperatorState
{
public:
    explicit EmitState(const RecordBuffer& resultBuffer) : resultBuffer(resultBuffer) { }

    nautilus::val<uint64_t> outputIndex = 0;
    RecordBuffer resultBuffer;
    /// Whether `execute` emitted a full buffer, which splits the output of the input into chunks.
    nautilus::val<bool> spilled = false;
};

EmitOperatorHandler& asEmitHandler(OperatorHandler* handler)
{
    PRECONDITION(handler != nullptr, "Expects a valid handler");
    return dynamic_cast<EmitOperatorHandler&>(*handler);
}

void setupHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pec, CompilationContext* compilationCtx)
{
    asEmitHandler(handler).setup(*compilationCtx, *pec);
}

void stopHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pec)
{
    asEmitHandler(handler).stop(QueryTerminationType::Graceful, *pec);
}

void emitSpilledProxy(OperatorHandler* handler, PipelineExecutionContext* pec, TupleBuffer* buffer, const ChunkNumber incomingChunkNumber)
{
    PRECONDITION(buffer != nullptr, "Expects a valid buffer");
    asEmitHandler(handler).emitSpilled(*buffer, incomingChunkNumber, *pec);
}

void emitClosingProxy(
    OperatorHandler* handler,
    PipelineExecutionContext* pec,
    TupleBuffer* buffer,
    const bool spilled,
    const ChunkNumber incomingChunkNumber,
    const bool isIncomingBufferTheLastChunk)
{
    PRECONDITION(buffer != nullptr, "Expects a valid buffer");
    asEmitHandler(handler).emitClosing(*buffer, spilled, incomingChunkNumber, isIncomingBufferTheLastChunk, *pec);
}

/// Writes the tuple count and the metadata of the current input into `recordBuffer`.
void setMetadata(const ExecutionContext& ctx, RecordBuffer& recordBuffer, const nautilus::val<uint64_t>& numRecords)
{
    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.watermarkTs);
    recordBuffer.setOriginId(ctx.originId);
    recordBuffer.setSequenceRange(ctx.sequenceNumber, ctx.sequenceRangeOffset);
    recordBuffer.setCreationTs(ctx.currentTs);
}
}

void EmitPhysicalOperator::setup(ExecutionContext& ctx, CompilationContext& compilationCtx) const
{
    nautilus::invoke(
        setupHandlerProxy,
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        ctx.pipelineContext,
        nautilus::val<CompilationContext*>{&compilationCtx});
}

void EmitPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    nautilus::invoke(stopHandlerProxy, ctx.getGlobalOperatorHandler(operatorHandlerId), ctx.pipelineContext);
}

void EmitPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer&) const
{
    /// initialize state variable and create new buffer
    const RecordBuffer resultBuffer{ctx.allocateBuffer()};
    auto emitState = std::make_unique<EmitState>(resultBuffer);
    ctx.setLocalOperatorState(id, std::move(emitState));
}

void EmitPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(id));

    /// We need to first check if the buffer has to be emitted and then write to it. Otherwise, it can happen that we will
    /// emit a tuple twice. Once in the execute() and then again in close(). This happens only for buffers that are filled
    /// to the brim, i.e., have no more space left.
    auto writeResult
        = bufferRef->writeRecord(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    /// An unsuccessful writeResult means, that the current record buffer is filled up completely and needs to be emitted first.
    /// We emit and create a new record buffer
    if (!writeResult.successful)
    {
        setMetadata(ctx, emitState->resultBuffer, emitState->outputIndex);
        nautilus::invoke(
            emitSpilledProxy,
            ctx.getGlobalOperatorHandler(operatorHandlerId),
            ctx.pipelineContext,
            emitState->resultBuffer.getReference(),
            ctx.chunkNumber);
        emitState->spilled = true;
        emitState->resultBuffer = RecordBuffer{ctx.allocateBuffer()};
        emitState->outputIndex = uint64_t{0};

        /// This write record call should succeed since a newly allocated tuple buffer should be able to store at least one record
        writeResult
            = bufferRef->writeRecord(emitState->outputIndex, emitState->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    }
    emitState->outputIndex = emitState->outputIndex + writeResult.writtenRecords;
}

void EmitPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    /// emit current buffer and set the metadata
    auto* const emitState = dynamic_cast<EmitState*>(ctx.getLocalState(id));
    setMetadata(ctx, emitState->resultBuffer, emitState->outputIndex);
    nautilus::invoke(
        emitClosingProxy,
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        ctx.pipelineContext,
        emitState->resultBuffer.getReference(),
        emitState->spilled,
        ctx.chunkNumber,
        ctx.lastChunk);
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
