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

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <Sequencing/SequenceRange.hpp>
#include <ExecutionContext.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <nautilus/function.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>
#include "Nautilus/Interface/TupleBufferProxyFunctions.hpp"

namespace NES
{

SequenceRange* getFracturedSequenceRangeForRawScanResult(size_t depthPlusOne)
{
    thread_local SequenceRange effectiveRange = SequenceRange::initial(depthPlusOne);
    return &effectiveRange;
}

ScanPhysicalOperator::ScanPhysicalOperator(
    std::shared_ptr<TupleBufferRef> bufferRef, std::vector<Record::RecordFieldIdentifier> projections)
    : bufferRef(std::move(bufferRef))
    , projections(std::move(projections))
    , isRawScan(std::dynamic_pointer_cast<InputFormatterTupleBufferRef>(this->bufferRef) != nullptr)
{
}

void ScanPhysicalOperator::rawScan(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatterTupleBufferRef>(this->bufferRef);

    auto effectiveRangeRef = RecordBuffer::SequenceRangeRef::from(
        nautilus::invoke(getFracturedSequenceRangeForRawScanResult, executionCtx.sequenceRange.getSize() + 1));
    effectiveRangeRef.setStart(0, executionCtx.sequenceRange.getStart(0));
    effectiveRangeRef.setEnd(0, executionCtx.sequenceRange.getEnd(0));
    executionCtx.sequenceRange = effectiveRangeRef;

    if (not inputFormatterBufferRef->indexBuffer(recordBuffer, executionCtx))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT);
        return;
    }

    if (inputFormatterBufferRef->isEmpty(recordBuffer))
    {
        executionCtx.setOpenReturnState(OpenReturnState::DISCARD);
        return;
    }


    /// call open on all child operators
    openChild(executionCtx, recordBuffer);

    /// process buffer
    const auto executeChildLambda = [this](ExecutionContext& executionCtx, Record& record)
    {
        executeChild(executionCtx, record);
    };
    inputFormatterBufferRef->readBuffer(executionCtx, recordBuffer, executeChildLambda);
}

void ScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceRange = recordBuffer.getSequenceRange();

    if (isRawScan)
    {
        rawScan(executionCtx, recordBuffer);
        return;
    }

    executionCtx.emitOnClose = true;
    /// call open on all child operators
    openChild(executionCtx, recordBuffer);
    /// iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    {
        auto record = bufferRef->readRecord(projections, recordBuffer, i);
        executeChild(executionCtx, record);
    }
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
