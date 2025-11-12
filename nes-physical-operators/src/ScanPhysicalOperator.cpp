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

#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/ComparisonFunctions/GreaterEqualsPhysicalFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/StdInt.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <SelectionPhysicalOperator.hpp>
#include <Utils.hpp>

namespace NES
{

ScanPhysicalOperator::ScanPhysicalOperator(
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
    std::vector<Record::RecordFieldIdentifier> projections)
    : memoryProvider(std::move(memoryProvider)), projections(std::move(projections))
{
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

    auto fieldNames = projections;
    if (getChild().has_value() && getChild()->tryGet<SelectionPhysicalOperator>().has_value())
    {
        auto selectionOp = getChild()->tryGet<SelectionPhysicalOperator>().value();
        //auto explain=  getChild().value().toString();
        auto func = selectionOp.getFunction();
        fieldNames = getAccessedFieldNames(func);
        //auto notInProjections = getVectorDifference(projections, fieldNames);
        //if (fieldNames.size() > 0 && fieldNames.size() < projections.size())//do only if not fitler over all fields

        recordBuffer.setTruncatedFields(true);
    }
    /// call open on all child operators
    openChild(executionCtx, recordBuffer);
    /// iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    {
        //TODO: if projection afterwards: get filter fields
        //if scan/emit around filter/agg/map (single op pipeline)
            //only read fields used/accessed in operator + index
            //execute on truncated projections

            //TODO: add "row_identifier" to record fields



            // only pass (or remove) all tuples with (no) index in results
            //alternatively add remaining projection fields to each record using memoryProvider(record index)

            //modify column buffer provider to writeRecords with added fields
            //TODO: also implement for row provider
        auto record = memoryProvider->readRecord(fieldNames, recordBuffer, i);
        // if (recordBuffer.getTruncatedFields())
        {
            record.write("row_identifier", nautilus::val(i));
        }
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
