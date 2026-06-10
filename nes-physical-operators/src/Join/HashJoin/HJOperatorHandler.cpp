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
#include <map>
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

namespace
{
/// Collects non-empty hash map buffers for one build side from a single slice
std::vector<TupleBuffer> getHashMapsFromSlice(const Slice& slice, JoinBuildSideType side)
{
    std::vector<TupleBuffer> buffers;
    const auto* hjSlice = dynamic_cast<const HJSlice*>(&slice);
    INVARIANT(hjSlice != nullptr, "Slice must be of type HJSlice!");
    for (uint64_t i = 0; i < hjSlice->getNumberOfHashMapsForSide(); ++i)
    {
        const auto* hashMapBuffer = hjSlice->getHashMapBufferRefForSide(WorkerThreadId(i), side);
        if (const auto hashMap = ChainedHashMap::load(*hashMapBuffer); hashMap.getTotalNumberOfRecords() > 0)
        {
            buffers.emplace_back(*hashMapBuffer);
        }
    }
    return buffers;
}

/// Collects non-empty hash map buffers from multiple slices for one build side
std::vector<TupleBuffer> getHashMapsFromSlices(const std::vector<std::shared_ptr<Slice>>& slices, JoinBuildSideType side)
{
    std::vector<TupleBuffer> allBuffers;
    for (const auto& slice : slices)
    {
        for (auto& buffer : getHashMapsFromSlice(*slice, side))
        {
            allBuffers.emplace_back(buffer);
        }
    }
    return allBuffers;
}
}

HJOperatorHandler::HJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets,
    JoinTriggerStrategy triggerStrategy)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
    , setupAlreadyCalledLeft(false)
    , setupAlreadyCalledRight(false)
    , leftRollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , rightRollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , maxNumberOfBuckets(maxNumberOfBuckets)
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
HJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Has setWorkerThreads() being called?");

    auto newHashMapArgs = dynamic_cast<const CreateNewHJSliceArgs&>(newSlicesArguments);
    switch (newHashMapArgs.joinBuildSide)
    {
        case JoinBuildSideType::Left:
            newHashMapArgs.numberOfBuckets = std::clamp(leftRollingAverageNumberOfKeys.rlock()->getAverage(), 1UL, maxNumberOfBuckets);
            break;
        case JoinBuildSideType::Right:
            newHashMapArgs.numberOfBuckets = std::clamp(rightRollingAverageNumberOfKeys.rlock()->getAverage(), 1UL, maxNumberOfBuckets);
            break;
    }

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
    const std::vector<std::shared_ptr<Slice>>& leftSlices,
    const std::vector<std::shared_ptr<Slice>>& rightSlices,
    ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    const auto leftHashMapBuffers = getHashMapsFromSlices(leftSlices, JoinBuildSideType::Left);
    const auto rightHashMapBuffers = getHashMapsFromSlices(rightSlices, JoinBuildSideType::Right);

    /// Update rolling average (accumulate locally, single lock acquisition)
    {
        uint64_t totalTuples = 0;
        uint64_t mapCount = 0;
        for (const auto& buffer : leftHashMapBuffers)
        {
            totalTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
            ++mapCount;
        }
        if (mapCount > 0)
        {
            leftRollingAverageNumberOfKeys.wlock()->add(totalTuples / mapCount);
        }

        /// Resetting before updating the right side
        totalTuples = 0;
        mapCount = 0;
        for (const auto& buffer : rightHashMapBuffers)
        {
            totalTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
            ++mapCount;
        }
        if (mapCount > 0)
        {
            rightRollingAverageNumberOfKeys.wlock()->add(totalTuples / mapCount);
        }
    }

    /// Creating a tuple buffer containing all necessary information for the probe
    uint64_t totalNumberOfTuples = 0;
    for (const auto& buffer : leftHashMapBuffers)
    {
        totalNumberOfTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
    }
    for (const auto& buffer : rightHashMapBuffers)
    {
        totalNumberOfTuples += ChainedHashMap::load(buffer).getTotalNumberOfRecords();
    }

    /// Hash map buffers are stored as child buffers, not inline in the main buffer.
    constexpr auto neededBufferSize = sizeof(EmittedHJWindowTrigger);
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededBufferSize);
    }

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
    new (tupleBuffer.getAvailableMemoryArea().data())
        EmittedHJWindowTrigger{windowInfo, leftHashMapBuffers.size(), rightHashMapBuffers.size(), probeTaskType};

    /// Store left hash map buffers as children first (indices 0..left-1), then right (indices left..left+right-1)
    for (auto leftBuffer : leftHashMapBuffers)
    {
        std::ignore = tupleBuffer.storeChildBuffer(leftBuffer);
    }
    for (auto rightBuffer : rightHashMapBuffers)
    {
        std::ignore = tupleBuffer.storeChildBuffer(rightBuffer);
    }

    pipelineCtx->emitBuffer(tupleBuffer);
}

}
