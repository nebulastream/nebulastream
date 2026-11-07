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

#include <Join/StreamJoinOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

UnboundedJoinSegments::UnboundedJoinSegments(const bool compactSegments) : compactSegments(compactSegments)
{
}

UnboundedJoinSegments::AppendedSlice
UnboundedJoinSegments::append(std::vector<TupleBuffer> newLeftBuffers, std::vector<TupleBuffer> newRightBuffers)
{
    AppendedSlice result;
    Compaction compaction;
    {
        const std::scoped_lock lock(mutex);
        for (const auto& segment : segments)
        {
            result.oldLeftBuffers.insert(result.oldLeftBuffers.end(), segment.leftBuffers.begin(), segment.leftBuffers.end());
            result.oldRightBuffers.insert(result.oldRightBuffers.end(), segment.rightBuffers.begin(), segment.rightBuffers.end());
        }
        segments.push_back(
            {.id = nextSegmentId++,
             .numberOfSlices = 1,
             .leftBuffers = std::move(newLeftBuffers),
             .rightBuffers = std::move(newRightBuffers)});
        while (segments.size() >= 2 and segments.back().numberOfSlices == segments[segments.size() - 2].numberOfSlices)
        {
            auto last = std::move(segments.back());
            segments.pop_back();
            auto& merged = segments.back();
            merged.id = nextSegmentId++;
            merged.numberOfSlices += last.numberOfSlices;
            std::ranges::move(last.leftBuffers, std::back_inserter(merged.leftBuffers));
            std::ranges::move(last.rightBuffers, std::back_inserter(merged.rightBuffers));
        }
        compaction = {.segmentId = segments.back().id, .leftBuffers = segments.back().leftBuffers, .rightBuffers = segments.back().rightBuffers};
    }

    if (not compactSegments)
    {
        return result;
    }
    /// A side that already consists of at most one buffer needs no compaction
    if (compaction.leftBuffers.size() <= 1)
    {
        compaction.leftBuffers.clear();
    }
    if (compaction.rightBuffers.size() <= 1)
    {
        compaction.rightBuffers.clear();
    }
    if (not compaction.leftBuffers.empty() or not compaction.rightBuffers.empty())
    {
        result.compaction = std::move(compaction);
    }
    return result;
}

void UnboundedJoinSegments::publishCompacted(
    const uint64_t segmentId, std::optional<TupleBuffer> leftBuffer, std::optional<TupleBuffer> rightBuffer)
{
    const std::scoped_lock lock(mutex);
    const auto segment = std::ranges::find(segments, segmentId, &Segment::id);
    if (segment == segments.end())
    {
        return;
    }
    if (leftBuffer)
    {
        segment->leftBuffers = {std::move(*leftBuffer)};
    }
    if (rightBuffer)
    {
        segment->rightBuffers = {std::move(*rightBuffer)};
    }
}

StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    SourcesOfInputOrigins sourcesOfInputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy,
    std::unique_ptr<UnboundedJoinSegments> unboundedSegments)
    : WindowBasedOperatorHandler(std::move(sourcesOfInputOrigins), outputOriginId, std::move(sliceAndWindowStore))
    , triggerStrategy(std::move(triggerStrategy))
    , unboundedSegments(std::move(unboundedSegments))
{
}

void StreamJoinOperatorHandler::publishCompactedSegment(
    const uint64_t segmentId, std::optional<TupleBuffer> leftBuffer, std::optional<TupleBuffer> rightBuffer)
{
    INVARIANT(unboundedSegments != nullptr, "Only a join with an UNBOUNDED window compacts its state");
    unboundedSegments->publishCompacted(segmentId, std::move(leftBuffer), std::move(rightBuffer));
}

void StreamJoinOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    if (unboundedSegments != nullptr)
    {
        triggerUnboundedSlices(slicesAndWindowInfo, pipelineCtx);
        return;
    }

    const EmitSlicesFn emitFn
        = [this](
              const std::vector<std::shared_ptr<Slice>>& leftSlices,
              const std::vector<std::shared_ptr<Slice>>& rightSlices,
              ProbeTaskType probeTaskType,
              const WindowInfo& windowInfo,
              const SequenceData& sequenceData,
              PipelineExecutionContext* ctx) { emitSlicesToProbe(leftSlices, rightSlices, probeTaskType, windowInfo, sequenceData, ctx); };

    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        triggerStrategy.triggerWindow(allSlices, windowInfo, emitFn, pipelineCtx);
    }
}

void StreamJoinOperatorHandler::triggerUnboundedSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, slices] : slicesAndWindowInfo)
    {
        auto newLeft = getJoinStateOfSlices(slices, JoinBuildSideType::Left);
        auto newRight = getJoinStateOfSlices(slices, JoinBuildSideType::Right);
        const auto appended = unboundedSegments->append(newLeft, newRight);
        const uint64_t totalChunks = appended.compaction.has_value() ? 3 : 2;

        /// New left tuples against all right tuples including the new ones, then all old left tuples against the new right tuples.
        /// Both iterate the new state, which is small, and look up in the large cumulative one.
        auto allRight = appended.oldRightBuffers;
        allRight.insert(allRight.end(), newRight.begin(), newRight.end());
        ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;
        const auto nextSequenceData = [&]
        {
            const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), chunkNumber == totalChunks};
            ++chunkNumber;
            return sequenceData;
        };
        emitJoinStateToProbe(
            newLeft, allRight, ProbeTaskType::MATCH_PAIRS_ITERATE_LEFT, windowInfo.windowInfo, nextSequenceData(), 0, pipelineCtx);
        emitJoinStateToProbe(
            appended.oldLeftBuffers,
            newRight,
            ProbeTaskType::MATCH_PAIRS_ITERATE_RIGHT,
            windowInfo.windowInfo,
            nextSequenceData(),
            0,
            pipelineCtx);
        if (appended.compaction.has_value())
        {
            emitJoinStateToProbe(
                appended.compaction->leftBuffers,
                appended.compaction->rightBuffers,
                ProbeTaskType::COMPACT,
                windowInfo.windowInfo,
                nextSequenceData(),
                appended.compaction->segmentId,
                pipelineCtx);
        }
    }
}

void StreamJoinOperatorHandler::emitJoinStateToProbe(
    const std::vector<TupleBuffer>& leftBuffers,
    const std::vector<TupleBuffer>& rightBuffers,
    const ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    const uint64_t segmentId,
    PipelineExecutionContext* pipelineCtx) const
{
    uint64_t totalNumberOfTuples = 0;
    for (const auto& buffer : leftBuffers)
    {
        totalNumberOfTuples += getNumberOfTuples(buffer);
    }
    for (const auto& buffer : rightBuffers)
    {
        totalNumberOfTuples += getNumberOfTuples(buffer);
    }

    /// The state buffers are stored as child buffers, not inline in the main buffer.
    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    INVARIANT(sizeof(EmittedJoinStateTrigger) <= tupleBuffer.getBufferSize(), "The trigger has to fit into a buffer");
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));

    /// Writing all necessary information for the probe to the buffer via the placement constructor
    new (tupleBuffer.getAvailableMemoryArea().data()) EmittedJoinStateTrigger{
        .windowInfo = windowInfo,
        .leftNumberOfBuffers = leftBuffers.size(),
        .rightNumberOfBuffers = rightBuffers.size(),
        .probeTaskType = probeTaskType,
        .segmentId = segmentId};

    /// Store the left buffers as children first (indices 0..left-1), then the right ones (indices left..left+right-1)
    for (auto leftBuffer : leftBuffers)
    {
        std::ignore = tupleBuffer.storeChildBuffer(leftBuffer);
    }
    for (auto rightBuffer : rightBuffers)
    {
        std::ignore = tupleBuffer.storeChildBuffer(rightBuffer);
    }

    pipelineCtx->emitBuffer(tupleBuffer);
}

void publishCompactedJoinStateProxy(
    OperatorHandler* operatorHandler,
    const uint64_t segmentId,
    TupleBuffer* leftBuffer,
    const bool hasLeft,
    TupleBuffer* rightBuffer,
    const bool hasRight)
{
    auto* handler = dynamic_cast<StreamJoinOperatorHandler*>(operatorHandler);
    INVARIANT(handler != nullptr, "A COMPACT task must belong to a join operator handler");
    handler->publishCompactedSegment(
        segmentId, hasLeft ? std::optional{*leftBuffer} : std::nullopt, hasRight ? std::optional{*rightBuffer} : std::nullopt);
}

}
