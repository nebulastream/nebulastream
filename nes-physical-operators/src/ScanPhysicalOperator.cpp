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

#include <memory>
#include <utility>
#include <vector>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Util.hpp>
#include <Util/StdInt.hpp>
#include <DefaultScanPhysicalOperator.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

DefaultScanPhysicalOperator::DefaultScanPhysicalOperator(
    std::shared_ptr<TupleBufferMemoryProvider> memoryProvider, std::vector<Record::RecordFieldIdentifier> projections)
    : memoryProvider(std::move(memoryProvider)), projections(std::move(projections))
{
}

void DefaultScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// initialize global state variables to keep track of the watermark ts and the origin id
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.originId = recordBuffer.getOriginId();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    /// call open on all child operators
    PhysicalOperatorConcept::open(executionCtx, recordBuffer);
    /// iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (nautilus::val<uint64_t> i = 0_u64; i < numberOfRecords; i = i + 1_u64)
    {
        auto record = memoryProvider->readRecord(projections, recordBuffer, i);
        PhysicalOperatorConcept::execute(executionCtx, record);
    }
}

}
