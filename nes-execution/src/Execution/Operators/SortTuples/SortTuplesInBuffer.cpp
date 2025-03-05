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
#include <cstdint>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SortTuples/SortTuplesInBuffer.hpp>

namespace NES::Runtime::Execution::Operators
{
SortTuplesInBuffer::SortTuplesInBuffer(
    std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProviderInput,
    std::vector<Nautilus::Record::RecordFieldIdentifier> projections,
    Nautilus::Record::RecordFieldIdentifier sortField)
    : memoryProviderInput(std::move(memoryProviderInput)), projections(std::move(projections)), sortField(std::move(sortField))
{
}

void SortTuplesInBuffer::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    /// call open on all child operators
    Operator::open(executionCtx, recordBuffer);


    /// "Sorting" the records in the buffer by passing on the records to the child operator that is currently the smallest.
    auto numberOfRecords = recordBuffer.getNumRecords();
    auto resultBuffer = RecordBuffer(executionCtx.allocateBuffer());
    nautilus::val<uint64_t> smallestIndex = 0;
    for (nautilus::val<uint64_t> i = 0; i < numberOfRecords; i = i + 1)
    {
        auto recordLastSmallest = memoryProviderInput->readRecord(projections, recordBuffer, smallestIndex);
        const auto curValue = recordLastSmallest.read(sortField);
        for (nautilus::val<uint64_t> j = 0; j < numberOfRecords; j = j + 1)
        {
            auto recordCandidateForSmallest = memoryProviderInput->readRecord(projections, recordBuffer, j);
            const auto value = recordCandidateForSmallest.read(sortField);
            if (value < curValue)
            {
                smallestIndex = j;
            }
        }
        auto recordToPass = memoryProviderInput->readRecord(projections, recordBuffer, smallestIndex);

        /// If the child operator is not nullptr, pass the record to it.
        if (child)
        {
            child->execute(executionCtx, recordToPass);
        }
    }
}
}