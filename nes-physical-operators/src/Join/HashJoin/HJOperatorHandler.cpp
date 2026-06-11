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


#include <Join/HashJoin/HJOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
HJOperatorHandler::HJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , setupAlreadyCalledLeft(false)
    , setupAlreadyCalledRight(false)
    , rollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , maxNumberOfBuckets(maxNumberOfBuckets)
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
HJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Has setWorkerThreads() being called?");

    auto newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    newHashMapArgs.numberOfBuckets = std::clamp(rollingAverageNumberOfKeys.rlock()->getAverage(), 1UL, maxNumberOfBuckets);
    return std::function(
        [bufferProvider = newHashMapArgs.bufferProvider,
         outputOriginId = outputOriginId,
         numberOfWorkerThreads = numberOfWorkerThreads,
         copyOfNewHashMapArgs = newHashMapArgs](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new hash-join slice for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<HJSlice>(*bufferProvider, sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads)};
        });
}

bool HJOperatorHandler::wasSetupCalled(const JoinBuildSideType& buildSide)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right: {
            bool expectedValue = false;
            return not setupAlreadyCalledRight.compare_exchange_strong(expectedValue, true);
        }
        case JoinBuildSideType::Left: {
            bool expectedValue = false;
            return not setupAlreadyCalledLeft.compare_exchange_strong(expectedValue, true);
        }
    }

    std::unreachable();
}

void HJOperatorHandler::emitSlicesToProbe(
    Slice& sliceLeft,
    Slice& sliceRight,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    /// Counting how many tuples the probe has to check for this probe task
    uint64_t totalNumberOfTuples = 0;

    /// Getting all hash maps for the left and right slice
    auto getHashMapsForSlice = [&](Slice& slice, const JoinBuildSideType& buildSide)
    {
        std::vector<TupleBuffer> allHashMapBuffers;
        auto* hashJoinSlice = dynamic_cast<HJSlice*>(&slice);
        INVARIANT(hashJoinSlice != nullptr, "Slice must be of type HashMapSlice!");
        for (uint64_t hashMapIdx = 0; hashMapIdx < hashJoinSlice->getNumberOfHashMapsForSide(); ++hashMapIdx)
        {
            const TupleBuffer* hashMapBuffer = hashJoinSlice->getHashMapBufferRefForSide(WorkerThreadId(hashMapIdx), buildSide);
            if (const ChainedHashMap hashMap = ChainedHashMap::load(*hashMapBuffer); hashMap.getTotalNumberOfRecords() > 0)
            {
                rollingAverageNumberOfKeys.wlock()->add(hashMap.getTotalNumberOfRecords());
                allHashMapBuffers.emplace_back(*hashMapBuffer);
                totalNumberOfTuples += hashMap.getTotalNumberOfRecords();
            }
        }
        return allHashMapBuffers;
    };
    const auto leftTupleBuffers = getHashMapsForSlice(sliceLeft, JoinBuildSideType::Left);
    const auto rightTupleBuffers = getHashMapsForSlice(sliceRight, JoinBuildSideType::Right);

    /// We need a buffer that is large enough to store:
    /// - size of EmittedHJWindowTrigger
    /// Hash map buffers are stored as child buffers, not inline in the main buffer.
    constexpr auto neededBufferSize = sizeof(EmittedHJWindowTrigger);
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededBufferSize);
    }

    /// As we are here "emitting" a buffer, we have to set the originId, the seq number, the watermark and the "number of tuples".
    /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
    auto tupleBuffer = tupleBufferVal.value();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));

    /// Writing all necessary information for the probe to the buffer via the placement constructor
    new (tupleBuffer.getAvailableMemoryArea().data()) EmittedHJWindowTrigger{windowInfo, leftTupleBuffers.size(), rightTupleBuffers.size()};

    /// Store left hash map buffers as children first (indices 0..left-1), then right (indices left..left+right-1)
    for (auto leftBuffer : leftTupleBuffers)
    {
        std::ignore = tupleBuffer.storeChildBuffer(leftBuffer);
    }
    for (auto rightBuffer : rightTupleBuffers)
    {
        std::ignore = tupleBuffer.storeChildBuffer(rightBuffer);
    }

    /// Dispatching the buffer to the probe operator via the task queue.
    pipelineCtx->emitBuffer(tupleBuffer);
    NES_TRACE(
        "Triggered window {}-{} with watermarkTs {} sequenceNumber {} originId {} and {}-{} hashmaps",
        windowInfo.windowStart,
        windowInfo.windowEnd,
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceNumber(),
        tupleBuffer.getOriginId(),
        leftTupleBuffers.size(),
        rightTupleBuffers.size());
}
}
