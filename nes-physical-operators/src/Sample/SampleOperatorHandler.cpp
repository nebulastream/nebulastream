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

#include <Sample/SampleOperatorHandler.hpp>

#include <chrono>
#include <memory>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

SampleOperatorHandler::SampleOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t valueSize,
    const uint64_t pageSize)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , setupAlreadyCalled(false)
    , valueSize(valueSize)
    , pageSize(pageSize)
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
SampleOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    const auto& hashMapSliceArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    return std::function(
        [hashMapSliceArgs,
         numberOfWorkerThreads = numberOfWorkerThreads](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<AggregationSlice>(sliceStart, sliceEnd, hashMapSliceArgs, numberOfWorkerThreads)}; });
}

void SampleOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        std::unique_ptr<ChainedHashMap> finalHashMap;
        std::vector<HashMap*> allHashMaps;
        uint64_t totalNumberOfTuples = 0;
        for (const auto& slice : allSlices)
        {
            const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
            for (uint64_t hashMapIdx = 0; hashMapIdx < aggregationSlice->getNumberOfHashMaps(); ++hashMapIdx)
            {
                if (auto* hashMap = aggregationSlice->getHashMapPtr(WorkerThreadId(hashMapIdx));
                    hashMap != nullptr and hashMap->getNumberOfTuples() > 0)
                {
                    allHashMaps.emplace_back(hashMap);
                    totalNumberOfTuples += hashMap->getNumberOfTuples();
                    if (not finalHashMap)
                    {
                        finalHashMap = ChainedHashMap::createNewMapWithSameConfiguration(*dynamic_cast<ChainedHashMap*>(hashMap));
                    }
                }
            }
        }
        if (not finalHashMap)
        {
            finalHashMap = std::make_unique<ChainedHashMap>(0, valueSize, NUMBER_OF_BUCKETS, pageSize);
        }

        const auto neededBufferSize = sizeof(EmittedAggregationWindow) + (allHashMaps.size() * sizeof(HashMap*));
        const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
        if (not tupleBufferVal.has_value())
        {
            throw CannotAllocateBuffer("{}B for the SAMPLE window trigger were requested", neededBufferSize);
        }
        auto tupleBuffer = tupleBufferVal.value();

        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setSequenceNumber(windowInfo.sequenceNumber);
        tupleBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        tupleBuffer.setLastChunk(true);
        tupleBuffer.setWatermark(windowInfo.windowInfo.windowStart);
        tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
        tupleBuffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));

        auto tmp = tupleBuffer.getAvailableMemoryArea();
        new (tmp.data()) EmittedAggregationWindow{windowInfo.windowInfo, std::move(finalHashMap), allHashMaps};

        pipelineCtx->emitBuffer(tupleBuffer);
    }
}

}
