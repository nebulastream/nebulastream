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

#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <ExecutionContext.hpp>
#include <InputFormatter.hpp>
#include <PhysicalOperator.hpp>
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

    if (not inputFormatterBufferRef->indexBuffer(
            recordBuffer, executionCtx.pipelineMemoryProvider.arena, executionCtx.runtimeInputFormatterRegistry))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT);
        return;
    }

    openChild(executionCtx, recordBuffer);

    const auto executeChildLambda = [this](ExecutionContext& executionCtx, Record& record) { executeChild(executionCtx, record); };
    inputFormatterBufferRef->readBuffer(executionCtx, recordBuffer, executeChildLambda);
}

void ScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
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
    openChild(executionCtx, recordBuffer);
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = uint64_t{0}; i < numberOfRecords; i = i + uint64_t{1})
    {
        auto record = bufferRef->readRecord(projections, recordBuffer, i);
        executeChild(executionCtx, record);
    }
}

bool ScanPhysicalOperator::hasRuntimeInputFormatter() const
{
    return std::dynamic_pointer_cast<InputFormatter>(bufferRef) != nullptr;
}

std::uintptr_t ScanPhysicalOperator::getRuntimeInputFormatterHandle() const
{
    if (const auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatter>(bufferRef))
    {
        return inputFormatterBufferRef->getRuntimeInputFormatterHandle();
    }
    return 0;
}

std::uintptr_t ScanPhysicalOperator::getRuntimeIndexerMetaDataHandle() const
{
    if (const auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatter>(bufferRef))
    {
        return inputFormatterBufferRef->getRuntimeIndexerMetaDataHandle();
    }
    return 0;
}

std::uintptr_t ScanPhysicalOperator::getRuntimeNullValuesHandle() const
{
    if (const auto inputFormatterBufferRef = std::dynamic_pointer_cast<InputFormatter>(bufferRef))
    {
        return inputFormatterBufferRef->getRuntimeNullValuesHandle();
    }
    return 0;
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
