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
#include <sstream>
#include <utility>
#include <vector>

#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <ExecutionContext.hpp>
#include <InputFormatterTupleBufferRef.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>

namespace NES
{

ScanPhysicalOperator::ScanPhysicalOperator(
    std::shared_ptr<TupleBufferRef> bufferRef, std::vector<Record::RecordFieldIdentifier> projections, uint64_t runtimeInputFormatterSlot)
    : bufferRef(std::move(bufferRef))
    , projections(std::move(projections))
    , isRawScan(std::dynamic_pointer_cast<InputFormatterTupleBufferRef>(this->bufferRef) != nullptr)
{
    (void)runtimeInputFormatterSlot;
}

void ScanPhysicalOperator::rawScan(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatterTupleBufferRef>(this->bufferRef);
    const auto runtimeInputFormatterHandle = inputFormatterBufferRef->getRuntimeInputFormatterHandle();

    if (not inputFormatterBufferRef->indexBuffer(recordBuffer, executionCtx.pipelineMemoryProvider.arena, runtimeInputFormatterHandle))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT);
        return;
    }

    /// call open on all child operators
    openChild(executionCtx, recordBuffer);

    /// process buffer
    const auto executeChildLambda = [this](ExecutionContext& executionCtx, Record& record) { executeChild(executionCtx, record); };
    inputFormatterBufferRef->readBuffer(executionCtx, recordBuffer, executeChildLambda, runtimeInputFormatterHandle);
}

void ScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();

    if (isRawScan)
    {
        rawScan(executionCtx, recordBuffer);
        return;
    }
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

std::shared_ptr<TupleBufferRef> ScanPhysicalOperator::getBufferRef() const
{
    return bufferRef;
}

std::uintptr_t ScanPhysicalOperator::getRuntimeInputFormatterHandle() const
{
    if (const auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatterTupleBufferRef>(bufferRef))
    {
        return inputFormatterBufferRef->getRuntimeInputFormatterHandle();
    }
    return 0;
}

std::string ScanPhysicalOperator::getSignature() const
{
    std::ostringstream signature;
    signature << "ScanPhysicalOperator(raw=" << isRawScan;
    if (bufferRef)
    {
        signature << ",capacity=" << bufferRef->getCapacity();
        signature << ",bufferSize=" << bufferRef->getBufferSize();
        signature << ",tupleSize=" << bufferRef->getTupleSize();
    }
    else
    {
        signature << ",bufferRef=<null>";
    }
    signature << ",projections=[";
    for (const auto& projection : projections)
    {
        signature << projection << ';';
    }
    signature << "])";
    return signature.str();
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
