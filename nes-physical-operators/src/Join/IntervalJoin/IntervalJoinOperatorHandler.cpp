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
#include <tuple>
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
#include <SliceStore/TimeBasedWindowSlicesStoreInterface.hpp>
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
/// No C++23 stdlib equivalent exists; replace with std::div_floor once the toolchain ships C++26 <intdiv> (P3724).
constexpr std::int64_t floorDiv(std::int64_t x, std::int64_t divisor)
{
    INVARIANT(divisor > 0, "floorDiv divisor must be positive; got {}", divisor);
    const auto q = x / divisor;
    const auto r = x % divisor;
    return (r != 0 && ((r < 0) != (divisor < 0))) ? q - 1 : q;
}

/// Computes ceil(x / divisor) for signed numerator and positive divisor.
/// No C++23 stdlib equivalent exists; replace with std::div_ceil once the toolchain ships C++26 <intdiv> (P3724).
constexpr std::int64_t ceilDiv(std::int64_t x, std::int64_t divisor)
{
    INVARIANT(divisor > 0, "ceilDiv divisor must be positive; got {}", divisor);
    const auto q = x / divisor;
    const auto r = x % divisor;
    return (r != 0 && ((r > 0) == (divisor > 0))) ? q + 1 : q;
}

}

IntervalJoinOperatorHandler::IntervalJoinOperatorHandler(
    std::vector<OriginId> anchorInputOriginsParam,
    std::vector<OriginId> partnerInputOriginsParam,
    const OriginId outputOriginIdParam,
    const IntervalBound lowerBoundParam,
    const IntervalBound upperBoundParam,
    SliceCacheConfiguration sliceCacheConfigurationParam,
    const bool emitAnchorNullFillParam,
    const bool emitPartnerNullFillParam)
    : lowerBound(lowerBoundParam)
    , upperBound(upperBoundParam)
    , sliceWidth(static_cast<std::uint64_t>(upperBoundParam.getRawValue() - lowerBoundParam.getRawValue()))
    , offsetLow(floorDiv(lowerBoundParam.getRawValue(), static_cast<std::int64_t>(sliceWidth)))
    , offsetHigh(ceilDiv(upperBoundParam.getRawValue(), static_cast<std::int64_t>(sliceWidth)))
    , anchorInputOrigins(std::move(anchorInputOriginsParam))
    , partnerInputOrigins(std::move(partnerInputOriginsParam))
    , outputOriginId(outputOriginIdParam)
    , sliceCacheConfiguration(std::move(sliceCacheConfigurationParam))
    , emitAnchorNullFill(emitAnchorNullFillParam)
    , emitPartnerNullFill(emitPartnerNullFillParam)
    , nextProbeSequence(SequenceNumber::INITIAL)
    , numberOfWorkerThreads(0)
    , anchorBuildDone(false)
    , partnerBuildDone(false)
    , terminationFlushed(false)
{
    PRECONDITION(
        lowerBound < upperBound,
        "Interval-join bounds require lowerBound < upperBound; got [{}, {}]",
        lowerBound.getRawValue(),
        upperBound.getRawValue());
    anchorStore = std::make_unique<IntervalSliceStore>(sliceWidth, sliceCacheConfiguration);
    partnerStore = std::make_unique<IntervalSliceStore>(sliceWidth, sliceCacheConfiguration);

    watermarkAnchor = std::make_unique<MultiOriginWatermarkProcessor>(anchorInputOrigins);
    watermarkPartner = std::make_unique<MultiOriginWatermarkProcessor>(partnerInputOrigins);
    watermarkProbe = std::make_unique<MultiOriginWatermarkProcessor>(std::vector{outputOriginId});
}

void IntervalJoinOperatorHandler::start(PipelineExecutionContext& pipelineCtx, std::uint32_t)
{
    /// Each pipeline (anchor build, partner build, probe) calls start. Storing
    /// the worker count repeatedly is safe — all pipelines share the same
    /// runtime worker pool, so the value is stable across calls.
    numberOfWorkerThreads.store(pipelineCtx.getNumberOfWorkerThreads(), std::memory_order_release);
}

void IntervalJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    /// Graceful flush happens on the build operators' terminate() chain via
    /// onSideBuildTerminated -> flushAllOnTermination. Nothing to do here.
}

void IntervalJoinOperatorHandler::notifyBufferDone(
    const IntervalJoinBuildSide side, const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    std::ignore = getBuildWatermark(side).updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    tryTriggerAnchors(pipelineCtx);
}

void IntervalJoinOperatorHandler::notifyBufferDoneProbe(const BufferMetaData& bufferMetaData)
{
    std::ignore = watermarkProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    const auto probeWatermark = watermarkProbe->getCurrentWatermark();

    /// RIGHT/FULL outer interval join performs a partner-anchored null-fill pass at termination that needs
    /// every anchor-side partner still present. Suppress incremental garbage collection so no partner is reclaimed beforehand.
    if (emitPartnerNullFill)
    {
        return;
    }

    anchorStore->garbageCollectTriggered(probeWatermark);

    /// Partner slices outlive their own end by max(0, -offsetLow) * sliceWidth so anchors with
    /// negative offsetLow can still see them. The garbage-collection threshold is probeWatermark shifted back by that.
    const auto partnerShift = std::max<std::int64_t>(0, -offsetLow) * static_cast<std::int64_t>(sliceWidth);
    const auto probeWatermarkRaw = static_cast<std::int64_t>(probeWatermark.getRawValue());
    if (probeWatermarkRaw > partnerShift)
    {
        partnerStore->garbageCollectExpired(Timestamp{static_cast<Timestamp::Underlying>(probeWatermarkRaw - partnerShift)});
    }
}

void IntervalJoinOperatorHandler::onSideBuildTerminated(const IntervalJoinBuildSide side, PipelineExecutionContext* pipelineCtx)
{
    switch (side)
    {
        case IntervalJoinBuildSide::Anchor:
            anchorBuildDone.store(true, std::memory_order_release);
            break;
        case IntervalJoinBuildSide::Partner:
            partnerBuildDone.store(true, std::memory_order_release);
            break;
    }

    if (anchorBuildDone.load(std::memory_order_acquire) && partnerBuildDone.load(std::memory_order_acquire))
    {
        /// Both sides may observe "both done" concurrently; exactly one thread must run the flush so all probe
        /// buffers are emitted within a single terminate() call (before that side's EOS), rather than being split
        /// across two threads where one thread's buffers can land after the other side's EOS and race teardown.
        if (!terminationFlushed.exchange(true, std::memory_order_acq_rel))
        {
            flushAllOnTermination(pipelineCtx);
            if (emitPartnerNullFill)
            {
                flushPartnerNullFill(pipelineCtx);
            }
        }
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

IntervalSliceStore& IntervalJoinOperatorHandler::getAnchorStore() noexcept
{
    return *anchorStore;
}

IntervalSliceStore& IntervalJoinOperatorHandler::getPartnerStore() noexcept
{
    return *partnerStore;
}

IntervalSliceStore& IntervalJoinOperatorHandler::getStore(const IntervalJoinBuildSide side) noexcept
{
    switch (side)
    {
        case IntervalJoinBuildSide::Anchor:
            return *anchorStore;
        case IntervalJoinBuildSide::Partner:
            return *partnerStore;
    }
    std::unreachable();
}

MultiOriginWatermarkProcessor& IntervalJoinOperatorHandler::getBuildWatermark(const IntervalJoinBuildSide side) noexcept
{
    switch (side)
    {
        case IntervalJoinBuildSide::Anchor:
            return *watermarkAnchor;
        case IntervalJoinBuildSide::Partner:
            return *watermarkPartner;
    }
    std::unreachable();
}

void IntervalJoinOperatorHandler::tryTriggerAnchors(PipelineExecutionContext* pipelineCtx)
{
    const auto anchorWatermark = watermarkAnchor->getCurrentWatermark();
    const auto partnerWatermark = watermarkPartner->getCurrentWatermark();

    /// Anchor S_A^i is triggerable iff
    ///   anchorWatermark  >= iEnd                                  (anchor itself built)
    ///   partnerWatermark >= iEnd + max(0, offsetHigh) * W         (partners built)
    const auto partnerShift = std::max<std::int64_t>(0, offsetHigh) * static_cast<std::int64_t>(sliceWidth);
    const auto partnerWatermarkRaw = static_cast<std::int64_t>(partnerWatermark.getRawValue());
    const auto anchorWatermarkRaw = static_cast<std::int64_t>(anchorWatermark.getRawValue());
    if (partnerWatermarkRaw < partnerShift)
    {
        /// Partner side hasn't reached the partner horizon for ANY anchor yet.
        return;
    }
    const auto effectiveWatermarkRaw = std::min(anchorWatermarkRaw, partnerWatermarkRaw - partnerShift);
    if (effectiveWatermarkRaw <= 0)
    {
        return;
    }
    const Timestamp effectiveWatermark{static_cast<Timestamp::Underlying>(effectiveWatermarkRaw)};

    auto anchors = anchorStore->claimTriggerable(effectiveWatermark);
    if (anchors.empty())
    {
        return;
    }

    assembleAndEmitTriggers(anchors, *partnerStore, offsetLow, offsetHigh, /*partnerNullFillPass=*/false, pipelineCtx);
}

void IntervalJoinOperatorHandler::flushAllOnTermination(PipelineExecutionContext* pipelineCtx)
{
    auto anchors = anchorStore->claimAllNonTriggered();
    if (anchors.empty())
    {
        return;
    }

    assembleAndEmitTriggers(anchors, *partnerStore, offsetLow, offsetHigh, /*partnerNullFillPass=*/false, pipelineCtx);
}

void IntervalJoinOperatorHandler::flushPartnerNullFill(PipelineExecutionContext* pipelineCtx)
{
    /// Anchor on the partner store; anchor-side partners are looked up symmetrically. A partner tuple at tP
    /// pairs with an anchor tuple at tA iff tA in [tP - upperBound, tP - lowerBound], i.e. the anchor-partner
    /// offset range is the normal range with the bounds negated and swapped.
    auto partnerAnchors = partnerStore->claimAllNonTriggered();
    if (partnerAnchors.empty())
    {
        return;
    }

    const auto widthSigned = static_cast<std::int64_t>(sliceWidth);
    const auto anchorOffsetLow = floorDiv(-upperBound.getRawValue(), widthSigned);
    const auto anchorOffsetHigh = ceilDiv(-lowerBound.getRawValue(), widthSigned);

    /// partnerNullFillPass=true emits even with zero anchor-side partners, so every partner tuple that matched no anchor tuple is null-filled.
    assembleAndEmitTriggers(partnerAnchors, *anchorStore, anchorOffsetLow, anchorOffsetHigh, /*partnerNullFillPass=*/true, pipelineCtx);
}

void IntervalJoinOperatorHandler::assembleAndEmitTriggers(
    std::vector<std::shared_ptr<IntervalJoinSlice>>& drivingSlices,
    IntervalSliceStore& partnerStoreParam,
    const std::int64_t offsetLowParam,
    const std::int64_t offsetHighParam,
    const bool partnerNullFillPass,
    PipelineExecutionContext* pipelineCtx)
{
    const auto widthSigned = static_cast<std::int64_t>(sliceWidth);
    for (auto& anchor : drivingSlices)
    {
        if (anchor->getNumberOfTuples() == 0)
        {
            continue;
        }
        anchor->combinePagedVectors();

        EmittedIntervalJoinWindowTrigger trigger{};
        trigger.anchorSliceEnd = anchor->getSliceEnd();
        trigger.windowInfo = WindowInfo{anchor->getSliceStart().getRawValue(), anchor->getSliceEnd().getRawValue()};
        trigger.partnerCount = 0;
        trigger.partnerNullFillPass = partnerNullFillPass;
        auto minPartnerStart = anchor->getSliceStart();

        const auto anchorStartRaw = static_cast<std::int64_t>(anchor->getSliceStart().getRawValue());
        const auto anchorBucketIndex = anchorStartRaw / widthSigned;
        for (std::int64_t k = offsetLowParam; k <= offsetHighParam; ++k)
        {
            const auto partnerEndRaw = (anchorBucketIndex + k + 1) * widthSigned;
            if (partnerEndRaw <= 0)
            {
                continue;
            }
            const SliceEnd partnerEnd{static_cast<Timestamp::Underlying>(partnerEndRaw)};
            const auto partnerOpt = partnerStoreParam.getSliceBySliceEnd(partnerEnd);
            if (!partnerOpt.has_value())
            {
                continue;
            }
            auto partnerSlice = std::dynamic_pointer_cast<IntervalJoinSlice>(partnerOpt.value());
            INVARIANT(partnerSlice != nullptr, "Interval-join slice store should hold IntervalJoinSlice instances");
            if (partnerSlice->getNumberOfTuples() == 0)
            {
                continue;
            }
            partnerSlice->combinePagedVectors();
            trigger.partnerSliceEnds[trigger.partnerCount++] = partnerEnd;
            minPartnerStart = std::min(minPartnerStart, partnerSlice->getSliceStart());
        }
        if (!partnerNullFillPass && trigger.partnerCount == 0 && !emitAnchorNullFill)
        {
            /// No non-empty partners and not a left-outer join — skip emit; sequence number is NOT consumed.
            /// For a left-outer join we still emit so the probe can null-fill the unmatched anchor tuples.
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

void IntervalJoinOperatorHandler::emitProbeBuffer(
    const EmittedIntervalJoinWindowTrigger& trigger,
    const SequenceData& sequenceData,
    const Timestamp watermark,
    PipelineExecutionContext* pipelineCtx) const
{
    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber{sequenceData.sequenceNumber});
    tupleBuffer.setChunkNumber(ChunkNumber{sequenceData.chunkNumber});
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(watermark);
    tupleBuffer.setNumberOfTuples(1);
    tupleBuffer.setCreationTimestampInMS(Timestamp{static_cast<Timestamp::Underlying>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count())});

    std::memcpy(tupleBuffer.getAvailableMemoryArea().data(), &trigger, sizeof(trigger));
    pipelineCtx->emitBuffer(tupleBuffer);

    NES_TRACE(
        "Emitted interval-join anchor end {} with {} partners (seq={}, watermark={}, outputOrigin={})",
        trigger.anchorSliceEnd,
        static_cast<std::uint64_t>(trigger.partnerCount),
        sequenceData.sequenceNumber,
        watermark,
        outputOriginId);
}

}
