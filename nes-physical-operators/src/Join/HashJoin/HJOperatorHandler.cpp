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
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
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
/// Collects non-empty hash maps for one build side from a single slice
std::vector<HashMap*> getHashMapsFromSlice(const Slice& slice, JoinBuildSideType side)
{
    std::vector<HashMap*> maps;
    const auto* hjSlice = dynamic_cast<const HJSlice*>(&slice);
    INVARIANT(hjSlice != nullptr, "Slice must be of type HJSlice!");
    for (uint64_t i = 0; i < hjSlice->getNumberOfHashMapsForSide(); ++i)
    {
        if (auto* map = hjSlice->getHashMapPtr(WorkerThreadId(i), side); map != nullptr && map->getNumberOfTuples() > 0)
        {
            maps.emplace_back(map);
        }
    }
    return maps;
}

/// Whether the given join type needs null-fill tasks for the specified side
bool needsNullFill(JoinLogicalOperator::JoinType joinType, JoinBuildSideType side)
{
    using JT = JoinLogicalOperator::JoinType;
    if (side == JoinBuildSideType::Left)
    {
        return joinType == JT::OUTER_LEFT_JOIN || joinType == JT::OUTER_FULL_JOIN;
    }
    return joinType == JT::OUTER_RIGHT_JOIN || joinType == JT::OUTER_FULL_JOIN;
}
}

HJOperatorHandler::HJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets,
    JoinLogicalOperator::JoinType joinType)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , setupAlreadyCalledLeft(false)
    , setupAlreadyCalledRight(false)
    , joinType(joinType)
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
        [outputOriginId = outputOriginId, numberOfWorkerThreads = numberOfWorkerThreads, copyOfNewHashMapArgs = newHashMapArgs](
            SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new hash-join slice for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<HJSlice>(sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads)};
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

void HJOperatorHandler::setNautilusCleanupExec(
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> nautilusCleanupExec, const JoinBuildSideType& buildSide)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right:
            rightCleanupStateNautilusFunction = std::move(nautilusCleanupExec);
            break;
        case JoinBuildSideType::Left:
            leftCleanupStateNautilusFunction = std::move(nautilusCleanupExec);
            break;
            std::unreachable();
    }
}

std::vector<std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec>> HJOperatorHandler::getNautilusCleanupExec() const
{
    return {leftCleanupStateNautilusFunction, rightCleanupStateNautilusFunction};
}

void HJOperatorHandler::emitProbeBuffer(
    const std::vector<HashMap*>& leftHashMaps,
    const std::vector<HashMap*>& rightHashMaps,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    ProbeTaskType probeTaskType,
    PipelineExecutionContext* pipelineCtx)
{
    uint64_t totalNumberOfTuples = 0;
    for (const auto* map : leftHashMaps)
    {
        totalNumberOfTuples += map->getNumberOfTuples();
    }
    for (const auto* map : rightHashMaps)
    {
        totalNumberOfTuples += map->getNumberOfTuples();
    }

    const auto neededBufferSize = sizeof(EmittedHJWindowTrigger) + ((leftHashMaps.size() + rightHashMaps.size()) * sizeof(HashMap*));
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

    new (tupleBuffer.getAvailableMemoryArea().data()) EmittedHJWindowTrigger{windowInfo, leftHashMaps, rightHashMaps, probeTaskType};

    pipelineCtx->emitBuffer(tupleBuffer);
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) outer join task emission requires iterating slices for match + null-fill
void HJOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    if (!isOuterJoin(joinType))
    {
        StreamJoinOperatorHandler::triggerSlices(slicesAndWindowInfo, pipelineCtx);
        return;
    }

    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        triggerOuterJoinWindow(windowInfo, allSlices, pipelineCtx);
    }
}

void HJOperatorHandler::triggerOuterJoinWindow(
    const WindowInfoAndSequenceNumber& windowInfo,
    const std::vector<std::shared_ptr<Slice>>& allSlices,
    PipelineExecutionContext* pipelineCtx)
{
    /// Collect all hash maps per side across all slices (needed for null-fill tasks)
    std::vector<HashMap*> allLeftHashMaps;
    std::vector<HashMap*> allRightHashMaps;
    for (const auto& slice : allSlices)
    {
        for (auto* map : getHashMapsFromSlice(*slice, JoinBuildSideType::Left))
        {
            allLeftHashMaps.emplace_back(map);
        }
        for (auto* map : getHashMapsFromSlice(*slice, JoinBuildSideType::Right))
        {
            allRightHashMaps.emplace_back(map);
        }
    }

    /// Update rolling average for bucket sizing
    for (const auto* map : allLeftHashMaps)
    {
        rollingAverageNumberOfKeys.wlock()->add(map->getNumberOfTuples());
    }
    for (const auto* map : allRightHashMaps)
    {
        rollingAverageNumberOfKeys.wlock()->add(map->getNumberOfTuples());
    }

    /// Calculate total number of chunks: S² match tasks + null-fill tasks
    const auto numSlices = allSlices.size();
    const auto matchTasks = numSlices * numSlices;
    uint64_t nullFillTasks = 0;
    if (needsNullFill(joinType, JoinBuildSideType::Left))
    {
        nullFillTasks += numSlices;
    }
    if (needsNullFill(joinType, JoinBuildSideType::Right))
    {
        nullFillTasks += numSlices;
    }
    const auto totalChunks = matchTasks + nullFillTasks;

    ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;

    /// 1) Emit S² match-pair tasks (same as inner join N×N)
    for (const auto& sliceLeft : allSlices)
    {
        for (const auto& sliceRight : allSlices)
        {
            const auto leftMaps = getHashMapsFromSlice(*sliceLeft, JoinBuildSideType::Left);
            const auto rightMaps = getHashMapsFromSlice(*sliceRight, JoinBuildSideType::Right);
            const bool isLast = chunkNumber == totalChunks;
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLast};
            emitProbeBuffer(leftMaps, rightMaps, windowInfo.windowInfo, sequenceData, ProbeTaskType::MATCH_PAIRS, pipelineCtx);
            ++chunkNumber;
        }
    }

    /// 2) Emit left-side null-fill tasks: one per slice, each checking one slice's left keys against ALL right maps
    if (needsNullFill(joinType, JoinBuildSideType::Left))
    {
        for (const auto& slice : allSlices)
        {
            const auto leftMapsForSlice = getHashMapsFromSlice(*slice, JoinBuildSideType::Left);
            const bool isLast = chunkNumber == totalChunks;
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLast};
            emitProbeBuffer(
                leftMapsForSlice, allRightHashMaps, windowInfo.windowInfo, sequenceData, ProbeTaskType::LEFT_NULL_FILL, pipelineCtx);
            ++chunkNumber;
        }
    }

    /// 3) Emit right-side null-fill tasks: one per slice, each checking one slice's right keys against ALL left maps
    if (needsNullFill(joinType, JoinBuildSideType::Right))
    {
        for (const auto& slice : allSlices)
        {
            const auto rightMapsForSlice = getHashMapsFromSlice(*slice, JoinBuildSideType::Right);
            const bool isLast = chunkNumber == totalChunks;
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLast};
            emitProbeBuffer(
                allLeftHashMaps, rightMapsForSlice, windowInfo.windowInfo, sequenceData, ProbeTaskType::RIGHT_NULL_FILL, pipelineCtx);
            ++chunkNumber;
        }
    }

    NES_TRACE(
        "Outer join: triggered window {}-{} with {} match tasks + {} null-fill tasks from {} slices",
        windowInfo.windowInfo.windowStart,
        windowInfo.windowInfo.windowEnd,
        matchTasks,
        nullFillTasks,
        numSlices);
}

void HJOperatorHandler::emitSlicesToProbe(
    Slice& sliceLeft,
    Slice& sliceRight,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    /// Getting all hash maps for the left and right slice
    auto getHashMapsForSlice = [&](const Slice& slice, const JoinBuildSideType& buildSide)
    {
        std::vector<HashMap*> allHashMaps;
        const auto* const hashJoinSlice = dynamic_cast<const HJSlice*>(&slice);
        INVARIANT(hashJoinSlice != nullptr, "Slice must be of type HashMapSlice!");
        for (uint64_t hashMapIdx = 0; hashMapIdx < hashJoinSlice->getNumberOfHashMapsForSide(); ++hashMapIdx)
        {
            if (auto* hashMap = hashJoinSlice->getHashMapPtr(WorkerThreadId(hashMapIdx), buildSide);
                hashMap and hashMap->getNumberOfTuples() > 0)
            {
                rollingAverageNumberOfKeys.wlock()->add(hashMap->getNumberOfTuples());
                allHashMaps.emplace_back(hashMap);
            }
        }
        return allHashMaps;
    };
    const auto leftHashMaps = getHashMapsForSlice(sliceLeft, JoinBuildSideType::Left);
    const auto rightHashMaps = getHashMapsForSlice(sliceRight, JoinBuildSideType::Right);

    emitProbeBuffer(leftHashMaps, rightHashMaps, windowInfo, sequenceData, ProbeTaskType::MATCH_PAIRS, pipelineCtx);
}

}
