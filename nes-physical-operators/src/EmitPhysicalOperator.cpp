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
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/FracturedNumber.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <nautilus/val.hpp>
#include <EmitOperatorHandler.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>
#include <function.hpp>
#include <val_ptr.hpp>
#include "Nautilus/DataTypes/DataTypesUtil.hpp"
#include "Nautilus/Interface/TupleBufferProxyFunctions.hpp"

namespace NES
{

#define log(NAME) nautilus::invoke(+[]() { NES_DEBUG(#NAME); })
/// Buffer to allocate the new SequenceNumberRange. Once Nautilus is able to dyna

SequenceRange* initializeOutputSequenceRangeProxy(size_t depthPlusOne)
{
    NES_DEBUG("Initializing sequence range: {}", depthPlusOne);
    thread_local SequenceRange range = SequenceRange::invalid();
    range = SequenceRange::initial(depthPlusOne);
    return &range;
}

RecordBuffer::SequenceRangeRef
getOutputSequenceRange(ExecutionContext& ctx, nautilus::val<size_t> outputBufferIndex, RecordBuffer::SequenceRangeRef& sequence)
{
    log(getOutputSequenceRange);
    for (nautilus::val<size_t> i = 0; i < sequence.getSize() - 1; ++i)
    {
        sequence.setStart(i, ctx.sequenceRange.getStart(i));
        sequence.setEnd(i, ctx.sequenceRange.getStart(i));
    }
    sequence.setStart(sequence.getSize() - 1, outputBufferIndex);
    sequence.setEnd(sequence.getSize() - 1, outputBufferIndex + 1);
    return sequence;
}

void marker()
{
}

RecordBuffer::SequenceRangeRef
getClosingOutputSequenceRange(ExecutionContext& ctx, nautilus::val<size_t> outputBufferIndex, RecordBuffer::SequenceRangeRef& sequence)
{
    for (nautilus::val<size_t> i = 0; i < sequence.getSize() - 1; ++i)
    {
        nautilus::invoke(marker);
        sequence.setStart(i, ctx.sequenceRange.getStart(i));
        sequence.setEnd(i, ctx.sequenceRange.getEnd(i));
    }
    sequence.setStart(sequence.getSize() - 1, outputBufferIndex);
    sequence.setEnd(sequence.getSize() - 1, 0);
    return sequence;
}

class EmitState : public OperatorState
{
public:
    explicit EmitState(const RecordBuffer& resultBuffer) : resultBuffer(resultBuffer), bufferMemoryArea(resultBuffer.getMemArea()) { }

    nautilus::val<uint64_t> outputIndex = 0;
    RecordBuffer resultBuffer;
    nautilus::val<int8_t*> bufferMemoryArea;
    nautilus::val<uint64_t> emittedBuffers = 0;
    RecordBuffer::SequenceRangeRef sequenceRange;
};

void EmitPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer&) const
{
    /// initialize state variable and create new buffer
    const auto resultBufferRef = ctx.allocateBuffer();
    const auto resultBuffer = RecordBuffer(resultBufferRef);
    auto allocated = nautilus::invoke(initializeOutputSequenceRangeProxy, ctx.sequenceRange.getSize() + 1);

    auto emitState = std::make_unique<EmitState>(resultBuffer);
    emitState->sequenceRange = RecordBuffer::SequenceRangeRef::from(allocated);
    nautilus::invoke(+[](size_t s) { NES_DEBUG("size: {}", s); }, emitState->sequenceRange.getSize());
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
        emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, emitState->emittedBuffers, emitState->sequenceRange, false);
        const auto resultBufferRef = ctx.allocateBuffer();
        emitState->resultBuffer = RecordBuffer(resultBufferRef);
        emitState->bufferMemoryArea = emitState->resultBuffer.getMemArea();
        emitState->outputIndex = 0_u64;
        emitState->emittedBuffers += 1;

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
    emitRecordBuffer(ctx, emitState->resultBuffer, emitState->outputIndex, emitState->emittedBuffers, emitState->sequenceRange, true);
}

void EmitPhysicalOperator::emitRecordBuffer(
    ExecutionContext& ctx,
    RecordBuffer& recordBuffer,
    const nautilus::val<uint64_t>& numRecords,
    nautilus::val<uint64_t>& outputBufferIndex,
    RecordBuffer::SequenceRangeRef& sequenceRange,
    bool closingBuffer) const
{
    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.watermarkTs);
    recordBuffer.setOriginId(ctx.originId);
    recordBuffer.setCreationTs(ctx.currentTs);


    if (closingBuffer)
    {
        recordBuffer.setSequenceRange(getClosingOutputSequenceRange(ctx, outputBufferIndex, sequenceRange));
    }
    else
    {
        recordBuffer.setSequenceRange(getOutputSequenceRange(ctx, outputBufferIndex, sequenceRange));
    }

    nautilus::invoke(+[](TupleBuffer* ref) { NES_DEBUG("Emitting: {}", ref->getSequenceRange()); }, recordBuffer.getReference());
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
