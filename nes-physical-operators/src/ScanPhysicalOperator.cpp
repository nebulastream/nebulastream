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

#include <Identifiers/Identifiers.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <InputFormatter.hpp>
#include <OriginMappingOperatorHandler.hpp>
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

ScanPhysicalOperator::ScanPhysicalOperator(
    std::shared_ptr<TupleBufferRef> bufferRef,
    std::vector<Record::RecordFieldIdentifier> projections,
    const OperatorHandlerId originMappingHandlerId)
    : bufferRef(std::move(bufferRef))
    , projections(std::move(projections))
    , isRawScan(std::dynamic_pointer_cast<InputFormatter>(this->bufferRef) != nullptr)
    , originMappingHandlerId(originMappingHandlerId)
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

void ScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    if (originMappingHandlerId.has_value())
    {
        /// This scan heads a branch of a fan-out point, so the records travel on under the id that branch carries
        /// rather than the one they were read with. Everything below reads the origin from the context, and the
        /// branch's emit writes it back onto the buffers it produces.
        executionCtx.originId = nautilus::invoke(
            +[](OperatorHandler* handler, const OriginId originId)
            {
                PRECONDITION(handler != nullptr, "Expects a valid handler");
                return dynamic_cast<OriginMappingOperatorHandler&>(*handler).mapOrigin(originId);
            },
            executionCtx.getGlobalOperatorHandler(originMappingHandlerId.value()),
            executionCtx.originId);
    }
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
    for (nautilus::val<uint64_t> i = uint64_t{0}; i < numberOfRecords; i = i + uint64_t{1})
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
