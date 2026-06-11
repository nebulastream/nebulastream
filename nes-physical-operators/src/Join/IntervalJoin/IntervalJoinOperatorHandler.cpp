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

#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Join/IntervalJoin/IntervalSliceStore.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

namespace
{

/// Computes floor(x / divisor) for signed numerator and positive divisor,
/// correctly rounding toward negative infinity. C's `/` rounds toward zero,
/// which would give the wrong slice index for negative bounds.
constexpr std::int64_t floorDiv(std::int64_t x, std::int64_t divisor)
{
    INVARIANT(divisor > 0, "floorDiv divisor must be positive; got {}", divisor);
    const auto q = x / divisor;
    const auto r = x % divisor;
    return (r != 0 && ((r < 0) != (divisor < 0))) ? q - 1 : q;
}

/// Computes ceil(x / divisor) for signed numerator and positive divisor.
constexpr std::int64_t ceilDiv(std::int64_t x, std::int64_t divisor)
{
    INVARIANT(divisor > 0, "ceilDiv divisor must be positive; got {}", divisor);
    const auto q = x / divisor;
    const auto r = x % divisor;
    return (r != 0 && ((r > 0) == (divisor > 0))) ? q + 1 : q;
}

}

IntervalJoinOperatorHandler::IntervalJoinOperatorHandler(
    std::vector<OriginId> leftInputOriginsParam,
    std::vector<OriginId> rightInputOriginsParam,
    const OriginId outputOriginIdParam,
    const std::int64_t lowerBoundParam,
    const std::int64_t upperBoundParam,
    SliceCacheConfiguration sliceCacheConfigurationParam)
    : lowerBound(lowerBoundParam)
    , upperBound(upperBoundParam)
    , widthW(static_cast<std::uint64_t>(upperBoundParam - lowerBoundParam))
    , offsetLow(floorDiv(lowerBoundParam, static_cast<std::int64_t>(widthW)))
    , offsetHigh(ceilDiv(upperBoundParam, static_cast<std::int64_t>(widthW)))
    , leftInputOrigins(std::move(leftInputOriginsParam))
    , rightInputOrigins(std::move(rightInputOriginsParam))
    , outputOriginId(outputOriginIdParam)
    , sliceCacheConfiguration(std::move(sliceCacheConfigurationParam))
    , nextProbeSequence(SequenceNumber::INITIAL)
    , numberOfWorkerThreads(0)
    , leftBuildDone(false)
    , rightBuildDone(false)
{
    PRECONDITION(lowerBound < upperBound, "Interval-join bounds require lowerBound < upperBound; got [{}, {}]", lowerBound, upperBound);
    leftStore = std::make_unique<IntervalSliceStore>(widthW, sliceCacheConfiguration);
    rightStore = std::make_unique<IntervalSliceStore>(widthW, sliceCacheConfiguration);

    auto combinedOrigins = leftInputOrigins;
    combinedOrigins.insert(combinedOrigins.end(), rightInputOrigins.begin(), rightInputOrigins.end());
    wmLeft = std::make_unique<MultiOriginWatermarkProcessor>(leftInputOrigins);
    wmRight = std::make_unique<MultiOriginWatermarkProcessor>(rightInputOrigins);
    wmProbe = std::make_unique<MultiOriginWatermarkProcessor>(std::vector{outputOriginId});
}

void IntervalJoinOperatorHandler::start(PipelineExecutionContext& pipelineCtx, std::uint32_t)
{
    /// Each pipeline (left build, right build, probe) calls start. Storing
    /// the worker count repeatedly is safe — all pipelines share the same
    /// runtime worker pool, so the value is stable across calls.
    numberOfWorkerThreads.store(pipelineCtx.getNumberOfWorkerThreads(), std::memory_order_release);
}

void IntervalJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    /// Graceful flush happens on the build operators' terminate() chain via
    /// onSideBuildTerminated -> flushAllOnTermination. Nothing to do here.
}

void IntervalJoinOperatorHandler::notifyBufferDoneLeft(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    [[maybe_unused]] const auto newWm
        = wmLeft->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    tryTriggerAnchors(pipelineCtx);
}

void IntervalJoinOperatorHandler::notifyBufferDoneRight(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    [[maybe_unused]] const auto newWm
        = wmRight->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    tryTriggerAnchors(pipelineCtx);
}

void IntervalJoinOperatorHandler::notifyBufferDoneProbe(const BufferMetaData& bufferMetaData)
{
    [[maybe_unused]] const auto newWm
        = wmProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    const auto probeWm = wmProbe->getCurrentWatermark();
    leftStore->garbageCollectTriggered(probeWm);

    /// Right slices outlive their own end by max(0, -offsetLow) * widthW so anchors with
    /// negative offsetLow can still see them. GC threshold is probeWm shifted back by that.
    const auto rightShift = std::max<std::int64_t>(0, -offsetLow) * static_cast<std::int64_t>(widthW);
    const auto probeWmRaw = static_cast<std::int64_t>(probeWm.getRawValue());
    if (probeWmRaw > rightShift)
    {
        rightStore->garbageCollectExpired(Timestamp{static_cast<Timestamp::Underlying>(probeWmRaw - rightShift)});
    }
}

void IntervalJoinOperatorHandler::onSideBuildTerminated(const JoinBuildSideType side, PipelineExecutionContext* pipelineCtx)
{
    if (side == JoinBuildSideType::Left)
    {
        leftBuildDone.store(true, std::memory_order_release);
    }
    else
    {
        rightBuildDone.store(true, std::memory_order_release);
    }

    if (leftBuildDone.load(std::memory_order_acquire) && rightBuildDone.load(std::memory_order_acquire))
    {
        flushAllOnTermination(pipelineCtx);
    }
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
IntervalJoinOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments&) const
{
    const auto numWorkers = numberOfWorkerThreads.load(std::memory_order_acquire);
    PRECONDITION(numWorkers > 0, "numberOfWorkerThreads not yet set; was IntervalJoinOperatorHandler::start() called?");
    return std::function(
        [numWorkers](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<IntervalJoinSlice>(sliceStart, sliceEnd, numWorkers)}; });
}

IntervalSliceStore& IntervalJoinOperatorHandler::getLeftStore() noexcept
{
    return *leftStore;
}

IntervalSliceStore& IntervalJoinOperatorHandler::getRightStore() noexcept
{
    return *rightStore;
}

void IntervalJoinOperatorHandler::tryTriggerAnchors(PipelineExecutionContext* pipelineCtx)
{
    const auto leftWm = wmLeft->getCurrentWatermark();
    const auto rightWm = wmRight->getCurrentWatermark();

    /// Anchor S_L^i is triggerable iff
    ///   leftWm  >= iEnd                                  (anchor itself built)
    ///   rightWm >= iEnd + max(0, offsetHigh) * W         (partners built)
    const auto rightShift = std::max<std::int64_t>(0, offsetHigh) * static_cast<std::int64_t>(widthW);
    const auto rightWmRaw = static_cast<std::int64_t>(rightWm.getRawValue());
    const auto leftWmRaw = static_cast<std::int64_t>(leftWm.getRawValue());
    if (rightWmRaw < rightShift)
    {
        /// Right side hasn't reached the partner horizon for ANY anchor yet.
        return;
    }
    const auto effectiveWmRaw = std::min(leftWmRaw, rightWmRaw - rightShift);
    if (effectiveWmRaw <= 0)
    {
        return;
    }
    const auto effectiveWm = Timestamp{static_cast<Timestamp::Underlying>(effectiveWmRaw)};

    auto anchors = leftStore->claimTriggerable(effectiveWm);
    if (anchors.empty())
    {
        return;
    }

    for (auto& anchor : anchors)
    {
        if (anchor->getNumberOfTuples() == 0)
        {
            continue;
        }
        anchor->combinePagedVectors();

        EmittedIntervalJoinWindowTrigger trigger{};
        trigger.leftSliceEnd = anchor->getSliceEnd();
        trigger.windowInfo = WindowInfo{anchor->getSliceStart().getRawValue(), anchor->getSliceEnd().getRawValue()};
        trigger.partnerCount = 0;
        auto minPartnerStart = anchor->getSliceStart();

        const auto anchorStartRaw = static_cast<std::int64_t>(anchor->getSliceStart().getRawValue());
        const auto widthSigned = static_cast<std::int64_t>(widthW);
        const auto iIndex = anchorStartRaw / widthSigned;
        for (std::int64_t k = offsetLow; k <= offsetHigh; ++k)
        {
            const auto partnerEndRaw = (iIndex + k + 1) * widthSigned;
            if (partnerEndRaw <= 0)
            {
                continue;
            }
            const auto partnerEnd = SliceEnd{static_cast<Timestamp::Underlying>(partnerEndRaw)};
            const auto partnerOpt = rightStore->getSliceBySliceEnd(partnerEnd);
            if (!partnerOpt.has_value())
            {
                continue;
            }
            auto partnerSlice = std::dynamic_pointer_cast<IntervalJoinSlice>(partnerOpt.value());
            INVARIANT(partnerSlice != nullptr, "Right store should hold IntervalJoinSlice instances");
            if (partnerSlice->getNumberOfTuples() == 0)
            {
                continue;
            }
            partnerSlice->combinePagedVectors();
            trigger.partnerSliceEnds[trigger.partnerCount++] = partnerEnd;
            minPartnerStart = std::min(minPartnerStart, partnerSlice->getSliceStart());
        }
        if (trigger.partnerCount == 0)
        {
            /// No non-empty partners — skip emit; sequence number is NOT consumed.
            continue;
        }

        const auto seqRaw = nextProbeSequence.fetch_add(1, std::memory_order_relaxed);
        const SequenceData sequenceData{SequenceNumber{seqRaw}, ChunkNumber{ChunkNumber::INITIAL}, /*lastChunk=*/true};

        /// Per-buffer watermark = min(anchorStart, min(partnerStarts)). Conservative; partner
        /// content predating the anchor still warrants the older timestamp.
        const auto bufferWatermark = std::min(anchor->getSliceStart(), minPartnerStart);

        emitProbeBuffer(trigger, sequenceData, bufferWatermark, pipelineCtx);
    }
}

void IntervalJoinOperatorHandler::flushAllOnTermination(PipelineExecutionContext* pipelineCtx)
{
    auto anchors = leftStore->claimAllNonTriggered();
    if (anchors.empty())
    {
        return;
    }

    for (auto& anchor : anchors)
    {
        if (anchor->getNumberOfTuples() == 0)
        {
            continue;
        }
        anchor->combinePagedVectors();

        EmittedIntervalJoinWindowTrigger trigger{};
        trigger.leftSliceEnd = anchor->getSliceEnd();
        trigger.windowInfo = WindowInfo{anchor->getSliceStart().getRawValue(), anchor->getSliceEnd().getRawValue()};
        trigger.partnerCount = 0;
        auto minPartnerStart = anchor->getSliceStart();

        const auto anchorStartRaw = static_cast<std::int64_t>(anchor->getSliceStart().getRawValue());
        const auto widthSigned = static_cast<std::int64_t>(widthW);
        const auto iIndex = anchorStartRaw / widthSigned;
        for (std::int64_t k = offsetLow; k <= offsetHigh; ++k)
        {
            const auto partnerEndRaw = (iIndex + k + 1) * widthSigned;
            if (partnerEndRaw <= 0)
            {
                continue;
            }
            const auto partnerEnd = SliceEnd{static_cast<Timestamp::Underlying>(partnerEndRaw)};
            const auto partnerOpt = rightStore->getSliceBySliceEnd(partnerEnd);
            if (!partnerOpt.has_value())
            {
                continue;
            }
            auto partnerSlice = std::dynamic_pointer_cast<IntervalJoinSlice>(partnerOpt.value());
            INVARIANT(partnerSlice != nullptr, "Right store should hold IntervalJoinSlice instances");
            if (partnerSlice->getNumberOfTuples() == 0)
            {
                continue;
            }
            partnerSlice->combinePagedVectors();
            trigger.partnerSliceEnds[trigger.partnerCount++] = partnerEnd;
            minPartnerStart = std::min(minPartnerStart, partnerSlice->getSliceStart());
        }
        if (trigger.partnerCount == 0)
        {
            continue;
        }

        const auto seqRaw = nextProbeSequence.fetch_add(1, std::memory_order_relaxed);
        const SequenceData sequenceData{SequenceNumber{seqRaw}, ChunkNumber{ChunkNumber::INITIAL}, /*lastChunk=*/true};
        const auto bufferWatermark = std::min(anchor->getSliceStart(), minPartnerStart);
        emitProbeBuffer(trigger, sequenceData, bufferWatermark, pipelineCtx);
    }
}

void IntervalJoinOperatorHandler::emitProbeBuffer(
    const EmittedIntervalJoinWindowTrigger& trigger,
    const SequenceData& sequenceData,
    const Timestamp watermark,
    PipelineExecutionContext* pipelineCtx) const
{
    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(watermark);
    tupleBuffer.setNumberOfTuples(1);
    tupleBuffer.setCreationTimestampInMS(Timestamp(static_cast<Timestamp::Underlying>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count())));

    std::memcpy(tupleBuffer.getAvailableMemoryArea().data(), &trigger, sizeof(trigger));
    pipelineCtx->emitBuffer(tupleBuffer);

    NES_TRACE(
        "Emitted interval-join anchor end {} with {} partners (seq={}, wm={}, outputOrigin={})",
        trigger.leftSliceEnd,
        static_cast<std::uint64_t>(trigger.partnerCount),
        sequenceData.sequenceNumber,
        watermark,
        outputOriginId);
}

}
