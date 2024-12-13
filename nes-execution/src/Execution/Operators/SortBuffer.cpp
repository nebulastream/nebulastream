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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SortBuffer.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
// #include <Execution/Operators/SortBuffer/SortBufferOperatorHandler.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/Logger/Logger.hpp>

#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators
{

SortBuffer::SortBuffer( //const uint64_t operatorHandlerIndex,
    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
    const Record::RecordFieldIdentifier& sortFieldIdentifier,
    const SortOrder sortOrder)
    : //operatorHandlerIndex(operatorHandlerIndex),
    memoryProvider(std::move(memoryProvider))
    , sortFieldIdentifier(sortFieldIdentifier)
    , sortOrder(sortOrder)
{
    // NES_ASSERT(this->memoryProvider->getMemoryLayoutPtr()->getFieldIndexFromName(sortFieldIdentifier).has_value(), "Sort field identifier not found.");
}

void SortBuffer::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    // Create new result buffer and initialize local state
    auto resultBuffer = RecordBuffer(executionCtx.allocateBuffer());
    // Sort recordBuffer by sortFieldIdentifier with modified out-of-place counting sort
    auto numberOfRecords = recordBuffer.getNumRecords();
    // auto bufferAddress = recordBuffer.getBuffer();
    for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    {
        nautilus::val<uint64_t> outputIndex = 0_u64;
        auto curRecord = memoryProvider->readRecord({}, recordBuffer, i);
        const auto& curValue = curRecord.read(sortFieldIdentifier);
        for (nautilus::val<uint64_t> j = 0_u64; j < numberOfRecords; j = j + 1_u64)
        {
            auto lesserOrGreaterRecord = memoryProvider->readRecord({}, recordBuffer, j);
            const auto& lesserOrGreaterValue = lesserOrGreaterRecord.read(sortFieldIdentifier);
            if (sortOrder == SortOrder::Ascending && (curValue > lesserOrGreaterValue || (curValue == lesserOrGreaterValue && i > j)))
            {
                outputIndex = outputIndex + 1_u64;
            }
            else if (sortOrder == SortOrder::Descending && (curValue < lesserOrGreaterValue || (curValue == lesserOrGreaterValue && i < j)))
            {
                outputIndex = outputIndex + 1_u64;
            }
        }
        memoryProvider->writeRecord(outputIndex, resultBuffer, curRecord);
    }

    // Emit sorted buffer
    emitRecordBuffer(executionCtx, recordBuffer, resultBuffer);
}

void SortBuffer::emitRecordBuffer(ExecutionContext& ctx, RecordBuffer& inputBuffer, RecordBuffer& ouputBuffer) const
{
    ouputBuffer.setNumRecords(inputBuffer.getNumRecords());
    ouputBuffer.setWatermarkTs(inputBuffer.getWatermarkTs());
    ouputBuffer.setOriginId(inputBuffer.getOriginId());
    ouputBuffer.setSequenceNumber(inputBuffer.getSequenceNumber());
    ouputBuffer.setChunkNumber(inputBuffer.getChunkNumber());
    ouputBuffer.setLastChunk(inputBuffer.isLastChunk());
    ouputBuffer.setCreationTs(inputBuffer.getCreatingTs());
    ctx.emitBuffer(ouputBuffer);
}
} // namespace NES::Runtime::Execution::Operators