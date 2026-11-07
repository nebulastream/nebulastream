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
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
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
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// Collects non-empty hash map buffers for one build side from a single slice. Read-only: a hash map that no
/// build worker ever touched for this slice is simply skipped, exactly as an untouched slot would be, rather
/// than being lazily created here. Trigger-time must never allocate, since that would race with a build worker
/// concurrently first-touching the same slot (see HashMapSlice::getOrCreateHashMapBufferRef).
std::vector<TupleBuffer> getHashMapsFromSlice(const Slice& slice, JoinBuildSideType side)
{
    std::vector<TupleBuffer> buffers;
    const auto* hjSlice = dynamic_cast<const HJSlice*>(&slice);
    INVARIANT(hjSlice != nullptr, "Slice must be of type HJSlice!");
    for (uint64_t i = 0; i < hjSlice->getNumberOfHashMapsForSide(); ++i)
    {
        if (const auto* hashMapBuffer = hjSlice->getHashMapBufferRefForSide(WorkerThreadId(i), side);
            hashMapBuffer != nullptr && ChainedHashMap::load(*hashMapBuffer).getTotalNumberOfRecords() > 0)
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
    SourcesOfInputOrigins sourcesOfInputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy,
    const bool unboundedWindow)
    : StreamJoinOperatorHandler(
          std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
    , setupAlreadyCalledLeft(false)
    , setupAlreadyCalledRight(false)
    , unboundedWindow(unboundedWindow)
{
}

void HJOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    if (unboundedWindow)
    {
        triggerUnboundedSlices(slicesAndWindowInfo, pipelineCtx);
        return;
    }
    StreamJoinOperatorHandler::triggerSlices(slicesAndWindowInfo, pipelineCtx);
}

/// Each pair of slices is joined by whichever of the two is appended to the segments last, and a slice with itself when it is
/// appended. Appends are serialized by the mutex, so every pair is joined exactly once, even if concurrent triggers append slices
/// out of event-time order.
void HJOperatorHandler::triggerUnboundedSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, slices] : slicesAndWindowInfo)
    {
        auto newLeft = getHashMapsFromSlices(slices, JoinBuildSideType::Left);
        auto newRight = getHashMapsFromSlices(slices, JoinBuildSideType::Right);

        std::vector<TupleBuffer> oldLeft;
        std::vector<TupleBuffer> oldRight;
        UnboundedJoinSegment toCompact;
        {
            const std::scoped_lock lock(unboundedSegmentsMutex);
            for (const auto& segment : unboundedSegments)
            {
                oldLeft.insert(oldLeft.end(), segment.leftHashMaps.begin(), segment.leftHashMaps.end());
                oldRight.insert(oldRight.end(), segment.rightHashMaps.begin(), segment.rightHashMaps.end());
            }
            unboundedSegments.push_back({.id = nextSegmentId++, .numberOfSlices = 1, .leftHashMaps = newLeft, .rightHashMaps = newRight});
            while (unboundedSegments.size() >= 2
                   and unboundedSegments.back().numberOfSlices == unboundedSegments[unboundedSegments.size() - 2].numberOfSlices)
            {
                auto last = std::move(unboundedSegments.back());
                unboundedSegments.pop_back();
                auto& merged = unboundedSegments.back();
                merged.id = nextSegmentId++;
                merged.numberOfSlices += last.numberOfSlices;
                std::ranges::move(last.leftHashMaps, std::back_inserter(merged.leftHashMaps));
                std::ranges::move(last.rightHashMaps, std::back_inserter(merged.rightHashMaps));
            }
            toCompact = unboundedSegments.back();
        }

        /// A side that already consists of at most one map needs no compaction
        if (toCompact.leftHashMaps.size() <= 1)
        {
            toCompact.leftHashMaps.clear();
        }
        if (toCompact.rightHashMaps.size() <= 1)
        {
            toCompact.rightHashMaps.clear();
        }
        const bool compact = not toCompact.leftHashMaps.empty() or not toCompact.rightHashMaps.empty();
        const uint64_t totalChunks = compact ? 3 : 2;

        /// New left tuples against all right tuples including the new ones, then all old left tuples against the new right tuples.
        /// Both iterate the new maps, which are small, and look up in the large cumulative ones.
        auto allRight = oldRight;
        allRight.insert(allRight.end(), newRight.begin(), newRight.end());
        ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;
        const auto nextSequenceData = [&]
        {
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), chunkNumber == totalChunks};
            ++chunkNumber;
            return sequenceData;
        };
        emitHashMapsToProbe(
            newLeft, allRight, ProbeTaskType::MATCH_PAIRS_ITERATE_LEFT, windowInfo.windowInfo, nextSequenceData(), 0, pipelineCtx);
        emitHashMapsToProbe(oldLeft, newRight, ProbeTaskType::MATCH_PAIRS, windowInfo.windowInfo, nextSequenceData(), 0, pipelineCtx);
        if (compact)
        {
            emitHashMapsToProbe(
                toCompact.leftHashMaps,
                toCompact.rightHashMaps,
                ProbeTaskType::COMPACT,
                windowInfo.windowInfo,
                nextSequenceData(),
                toCompact.id,
                pipelineCtx);
        }
    }
}

void HJOperatorHandler::publishCompactedSegment(
    const uint64_t segmentId, std::optional<TupleBuffer> leftHashMap, std::optional<TupleBuffer> rightHashMap)
{
    const std::scoped_lock lock(unboundedSegmentsMutex);
    const auto segment = std::ranges::find(unboundedSegments, segmentId, &UnboundedJoinSegment::id);
    if (segment == unboundedSegments.end())
    {
        return;
    }
    if (leftHashMap)
    {
        segment->leftHashMaps = {std::move(*leftHashMap)};
    }
    if (rightHashMap)
    {
        segment->rightHashMaps = {std::move(*rightHashMap)};
    }
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
HJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Has setWorkerThreads() being called?");

    const auto& newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
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

void HJOperatorHandler::emitSlicesToProbe(
    const std::vector<std::shared_ptr<Slice>>& leftSlices,
    const std::vector<std::shared_ptr<Slice>>& rightSlices,
    ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    emitHashMapsToProbe(
        getHashMapsFromSlices(leftSlices, JoinBuildSideType::Left),
        getHashMapsFromSlices(rightSlices, JoinBuildSideType::Right),
        probeTaskType,
        windowInfo,
        sequenceData,
        0,
        pipelineCtx);
}

void HJOperatorHandler::emitHashMapsToProbe(
    const std::vector<TupleBuffer>& leftHashMapBuffers,
    const std::vector<TupleBuffer>& rightHashMapBuffers,
    ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    const uint64_t segmentId,
    PipelineExecutionContext* pipelineCtx) const
{
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
        EmittedHJWindowTrigger{windowInfo, leftHashMapBuffers.size(), rightHashMapBuffers.size(), probeTaskType, segmentId};

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
