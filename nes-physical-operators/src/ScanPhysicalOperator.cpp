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

#include <Interface/MemoryLayout/MemoryLayout.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordView.hpp>
#include <Interface/TaskBufferRef.hpp>
#include <ExecutionContext.hpp>
#include <InputFormatter.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>
#include <val_arith.hpp>

namespace NES
{

ScanPhysicalOperator::ScanPhysicalOperator(std::shared_ptr<MemoryLayout> layout, std::vector<Record::RecordFieldIdentifier> projections)
    : layout(std::move(layout))
    , projections(std::move(projections))
    , isRawScan(std::dynamic_pointer_cast<InputFormatter>(this->layout) != nullptr)
{
}

void ScanPhysicalOperator::rawScan(ExecutionContext& executionCtx, TaskBufferRef& recordBuffer) const
{
    auto inputFormatter = std::dynamic_pointer_cast<InputFormatter>(this->layout);

    if (not inputFormatter->indexBuffer(recordBuffer, executionCtx.pipelineMemoryProvider.arena))
    {
        executionCtx.setOpenReturnState(OpenReturnState::REPEAT);
        return;
    }

    /// call open on all child operators
    openChild(executionCtx, recordBuffer);

    /// process buffer
    const auto executeChildLambda = [this](ExecutionContext& executionCtx, Record& record) { executeChild(executionCtx, record); };
    inputFormatter->readBuffer(executionCtx, recordBuffer, executeChildLambda);
}

void ScanPhysicalOperator::open(ExecutionContext& executionCtx, TaskBufferRef& recordBuffer) const
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
    const RecordView view{recordBuffer, layout};
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = uint64_t{0}; i < numberOfRecords; i = i + uint64_t{1})
    {
        auto record = view.readRecord(projections, i);
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
