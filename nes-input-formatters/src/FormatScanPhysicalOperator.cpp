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


#include <InputFormatters/FormatScanPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

#include "InputFormatterTask.hpp"
#include "NativeInputFormatIndexer.hpp"
#include "RawValueParser.hpp"

namespace NES
{


FormatScanPhysicalOperator::FormatScanPhysicalOperator(
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
    std::vector<Record::RecordFieldIdentifier> projections,
    std::unique_ptr<InputFormatters::InputFormatterTaskPipeline> inputFormatterTaskPipeline)
    : memoryProvider(std::move(memoryProvider)), projections(std::move(projections)), taskPipeline(std::move(inputFormatterTaskPipeline))
{
}

void FormatScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    // Todo: the goal is to call the input formatter task pipeline here (essentially its execute function, which we could rename to scan/index
    // First step:
    //  - support below scan functionality via 'IsFormattingRequired' and 'not(HasSpanningTuples)' case
    //  - Requirements:
    //      - NativeFieldIndexFunction with below nautilus function
    //          - readRecord(projections) should be a call on the FieldIndexFunction
    //          - checkout the RowTupleBufferMemoryProvider for the concrete implementation of 'readRecord'
    //              - essentially move the 'RowTupleBufferMemoryProvider' to the RowNativeFieldIndexFunction (or have one and template it)

    // Todo: remove both getter functions again and change interface of InputFormatterTask
    this->taskPipeline->scan(executionCtx, recordBuffer, child.value(), *memoryProvider, projections);

    // /// initialize global state variables to keep track of the watermark ts and the origin id
    // executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    // executionCtx.originId = recordBuffer.getOriginId();
    // executionCtx.currentTs = recordBuffer.getCreatingTs();
    // executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    // executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    // executionCtx.lastChunk = recordBuffer.isLastChunk();
    // /// call open on all child operators
    // openChild(executionCtx, recordBuffer);
    // /// iterate over records in buffer
    // auto numberOfRecords = recordBuffer.getNumRecords();
    //
    // // Todo: instead of this for loop,
    // for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    // {
    //     auto record = memoryProvider->readRecord(projections, recordBuffer, i);
    //     executeChild(executionCtx, record);
    // }
}

std::optional<PhysicalOperator> FormatScanPhysicalOperator::getChild() const
{
    return child;
}

void FormatScanPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
