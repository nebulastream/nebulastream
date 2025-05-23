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
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Streaming/Aggregation/AggregationSlice.hpp>
#include <Streaming/WindowBasedOperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

AggregationOperatorHandler::AggregationOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
{
}

void AggregationOperatorHandler::setHashMapParams(
    const uint64_t keySize, const uint64_t valueSize, const uint64_t pageSize, const uint64_t numberOfBuckets)
{
    this->keySize = keySize;
    this->valueSize = valueSize;
    this->pageSize = pageSize;
    this->numberOfBuckets = numberOfBuckets;
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> AggregationOperatorHandler::getCreateNewSlicesFunction() const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    return std::function(
        [keySize = keySize,
         valueSize = valueSize,
         numberOfBuckets = numberOfBuckets,
         pageSize = pageSize,
         outputOriginId = outputOriginId,
         numberOfWorkerThreads = numberOfWorkerThreads](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new aggregation slice with for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<AggregationSlice>(
                keySize, valueSize, numberOfBuckets, pageSize, sliceStart, sliceEnd, numberOfWorkerThreads)};
        });
}

void AggregationOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        /// Getting all hashmaps for each slice that has at least one tuple
        std::vector<Nautilus::Interface::HashMap*> allHashMaps;
        uint64_t totalNumberOfTuples = 0;
        for (const auto& slice : allSlices)
        {
            const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
            for (uint64_t hashMapIdx = 0; hashMapIdx < aggregationSlice->getNumberOfHashMaps(); ++hashMapIdx)
            {
                if (auto* hashmap = aggregationSlice->getHashMapPtr(WorkerThreadId(hashMapIdx)); hashmap->getNumberOfTuples() > 0)
                {
                    allHashMaps.emplace_back(hashmap);
                    totalNumberOfTuples += hashmap->getNumberOfTuples();
                }
            }
        }


        /// We need a buffer that is large enough to store:
        /// - all pointers to all hashmaps of the window to be triggered
        /// - a new hashmap for the probe operator, so that we are not overwriting the thread local hashmaps
        /// - size of EmittedAggregationWindow
        const auto neededBufferSize = sizeof(EmittedAggregationWindow) + (allHashMaps.size() * sizeof(Nautilus::Interface::HashMap*));
        Memory::TupleBuffer tupleBuffer;
        if (pipelineCtx->getBufferManager()->getBufferSize() >= neededBufferSize)
        {
            tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
        }
        else
        {
            const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
            if (not tupleBufferVal.has_value())
            {
                throw CannotAllocateBuffer("Could not get a buffer of size {} for the aggregation window trigger", neededBufferSize);
            }
            tupleBuffer = tupleBufferVal.value();
        }
        /// It might be that the buffer is not zeroed out.
        std::memset(tupleBuffer.getBuffer(), 0, neededBufferSize);

        /// As we are here "emitting" a buffer, we have to set the originId, the seq number, the watermark and the "number of tuples".
        /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setSequenceNumber(windowInfo.sequenceNumber);
        tupleBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        tupleBuffer.setLastChunk(true);
        tupleBuffer.setWatermark(windowInfo.windowInfo.windowStart);
        tupleBuffer.setNumberOfTuples(totalNumberOfTuples);


        /// Writing all necessary information for the aggregation probe to the buffer.
        auto* bufferMemory = tupleBuffer.getBuffer<EmittedAggregationWindow>();
        bufferMemory->windowInfo = windowInfo.windowInfo;
        bufferMemory->numberOfHashMaps = allHashMaps.size();
        bufferMemory->finalHashMap = std::make_unique<Nautilus::Interface::ChainedHashMap>(keySize, valueSize, numberOfBuckets, pageSize);
        auto* addressFirstHashMapPtr = reinterpret_cast<int8_t*>(bufferMemory) + sizeof(EmittedAggregationWindow);
        bufferMemory->hashMaps = reinterpret_cast<Nautilus::Interface::HashMap**>(addressFirstHashMapPtr);
        std::memcpy(addressFirstHashMapPtr, allHashMaps.data(), allHashMaps.size() * sizeof(Nautilus::Interface::HashMap*));


        /// Dispatching the buffer to the probe operator via the task queue.
        pipelineCtx->emitBuffer(tupleBuffer);
        NES_TRACE(
            "Emitted window {}-{} with watermarkTs {} sequenceNumber {} originId {}",
            windowInfo.windowInfo.windowStart,
            windowInfo.windowInfo.windowEnd,
            tupleBuffer.getWatermark(),
            tupleBuffer.getSequenceNumber(),
            tupleBuffer.getOriginId());
    }
}

}
