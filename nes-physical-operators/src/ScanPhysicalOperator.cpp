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


#include <ScanPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <DataTypes/DataTypesUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <InputCollector.hpp>
#include <InputFormatter.hpp>
#include <PhysicalOperator.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>

namespace NES
{

ScanPhysicalOperator::ScanPhysicalOperator(
    std::shared_ptr<TupleBufferRef> bufferRef, std::vector<Record::RecordFieldIdentifier> projections)
    : bufferRef(std::move(bufferRef))
    , projections(std::move(projections))
    , isRawScan(std::dynamic_pointer_cast<InputFormatter>(this->bufferRef) != nullptr)
{
}

void ScanPhysicalOperator::rawScan(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatter>(this->bufferRef);

    if (not inputFormatterBufferRef->indexBuffer(recordBuffer, executionCtx.pipelineMemoryProvider.arena))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT);
        return;
    }

    /// call open on all child operators
    openChild(executionCtx, recordBuffer);

    /// process buffer
    const auto executeChildLambda = [this](ExecutionContext& executionCtx, Record& record) { executeChild(executionCtx, record); };
    inputFormatterBufferRef->readBuffer(executionCtx, recordBuffer, executeChildLambda);
}

void ScanPhysicalOperator::setCollector(const OperatorHandlerId collector)
{
    this->collector = collector;
}

void ScanPhysicalOperator::scanRecords(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = uint64_t{0}; i < numberOfRecords; i = i + uint64_t{1})
    {
        auto record = bufferRef->readRecord(projections, recordBuffer, i);
        executeChild(executionCtx, record);
    }
}

void ScanPhysicalOperator::collectingOpen(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    using Unit = InputCollector::Unit;
    const auto handler = executionCtx.getGlobalOperatorHandler(*collector);
    const auto unit = nautilus::invoke(InputCollector::collectProxy, handler, recordBuffer.getReference());
    if (unit == nautilus::val<const Unit*>(nullptr))
    {
        executionCtx.setOpenReturnState(OpenReturnState::HOLD);
        /// A held input skips `close`.
        nautilus::invoke(InputCollector::repeatIfArmedProxy, executionCtx.pipelineContext, recordBuffer.getReference());
        return;
    }
    executionCtx.originId = nautilus::val<OriginId>(*getMemberWithOffset<OriginId::Underlying>(unit, offsetof(Unit, origin)));
    executionCtx.watermarkTs = nautilus::val<Timestamp>(*getMemberWithOffset<Timestamp::Underlying>(unit, offsetof(Unit, watermark)));
    executionCtx.currentTs = nautilus::val<Timestamp>(*getMemberWithOffset<Timestamp::Underlying>(unit, offsetof(Unit, creation)));
    executionCtx.sequenceNumber
        = nautilus::val<SequenceNumber>(*getMemberWithOffset<SequenceNumber::Underlying>(unit, offsetof(Unit, sequenceNumber)));
    executionCtx.sequenceRangeOffset = *getMemberWithOffset<uint32_t>(unit, offsetof(Unit, rangeOffset));
    executionCtx.chunkNumber = nautilus::val<ChunkNumber>(*getMemberWithOffset<ChunkNumber::Underlying>(unit, offsetof(Unit, chunkNumber)));
    executionCtx.lastChunk = *getMemberWithOffset<bool>(unit, offsetof(Unit, lastChunk));
    /// The children's `open` and `close` receive the input that triggered the task, whose metadata may differ from the unit's.
    openChild(executionCtx, recordBuffer);
    const nautilus::val<const TupleBuffer*> buffers = *getMemberWithOffset<const TupleBuffer*>(unit, offsetof(Unit, buffers));
    const nautilus::val<uint64_t> numberOfBuffers = *getMemberWithOffset<uint64_t>(unit, offsetof(Unit, numberOfBuffers));
    for (nautilus::val<uint64_t> b = uint64_t{0}; b < numberOfBuffers; b = b + uint64_t{1})
    {
        RecordBuffer unitBuffer{BorrowedNautilusBuffer::from(buffers + b)};
        scanRecords(executionCtx, unitBuffer);
    }
    nautilus::invoke(InputCollector::releaseUnitProxy);
}

void ScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (collector)
    {
        collectingOpen(executionCtx, recordBuffer);
        return;
    }
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();

    if (isRawScan)
    {
        /// Raw buffers cover one sequence number (@see InputFormatter).
        rawScan(executionCtx, recordBuffer);
        return;
    }
    executionCtx.sequenceRangeOffset = recordBuffer.getSequenceRangeOffset();
    /// call open on all child operators
    openChild(executionCtx, recordBuffer);
    scanRecords(executionCtx, recordBuffer);
}

void ScanPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    closeChild(executionCtx, recordBuffer);
    if (collector)
    {
        /// A repeated task must not emit, so the repeat follows the children's close.
        nautilus::invoke(InputCollector::repeatIfArmedProxy, executionCtx.pipelineContext, recordBuffer.getReference());
    }
}

void ScanPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    if (collector)
    {
        nautilus::invoke(InputCollector::stopProxy, executionCtx.getGlobalOperatorHandler(*collector), executionCtx.pipelineContext);
    }
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> ScanPhysicalOperator::getChild() const
{
    return child;
}

void ScanPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
