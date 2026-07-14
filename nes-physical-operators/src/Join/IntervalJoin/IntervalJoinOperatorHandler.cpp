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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/IntervalSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/IntervalBound.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

namespace
{

/// Applies the signed `bucketOffset` to an unsigned bucket index. Returns nullopt when the partner bucket
/// would fall before bucket 0 or past `maxBucketIndex`, i.e. outside the unsigned timestamp range, so no partner
/// slice can exist. Keeping this unsigned matters: narrowing a timestamp to int64_t flips everything at or above
/// INT64_MAX negative.
constexpr std::optional<std::uint64_t>
partnerBucketIndexFor(const std::uint64_t anchorBucketIndex, const std::int64_t bucketOffset, const std::uint64_t maxBucketIndex)
{
    /// |bucketOffset| without signed negation, which is UB for INT64_MIN.
    const std::uint64_t offsetMagnitude
        = bucketOffset < 0 ? std::uint64_t{0} - static_cast<std::uint64_t>(bucketOffset) : static_cast<std::uint64_t>(bucketOffset);
    if (bucketOffset < 0)
    {
        return offsetMagnitude > anchorBucketIndex ? std::nullopt : std::optional{anchorBucketIndex - offsetMagnitude};
    }
    if (anchorBucketIndex > maxBucketIndex || offsetMagnitude > maxBucketIndex - anchorBucketIndex)
    {
        return std::nullopt;
    }
    return anchorBucketIndex + offsetMagnitude;
}

/// floor(x / divisor) rounding toward -inf (C's `/` rounds toward zero -> wrong slice index for negative bounds).
/// Replace with std::div_floor once the toolchain ships C++26 <intdiv> (P3724).
constexpr std::int64_t floorDiv(std::int64_t dividend, std::int64_t divisor)
{
    INVARIANT(divisor > 0, "floorDiv divisor must be positive; got {}", divisor);
    const auto quotient = dividend / divisor;
    const auto remainder = dividend % divisor;
    return (remainder != 0 && ((remainder < 0) != (divisor < 0))) ? quotient - 1 : quotient;
}

/// ceil(x / divisor). Replace with std::div_ceil once the toolchain ships C++26 <intdiv> (P3724).
constexpr std::int64_t ceilDiv(std::int64_t dividend, std::int64_t divisor)
{
    INVARIANT(divisor > 0, "ceilDiv divisor must be positive; got {}", divisor);
    const auto quotient = dividend / divisor;
    const auto remainder = dividend % divisor;
    return (remainder != 0 && ((remainder > 0) == (divisor > 0))) ? quotient + 1 : quotient;
}

}

IntervalJoinOperatorHandler::IntervalJoinOperatorHandler(
    std::vector<OriginId> anchorInputOriginsParam,
    std::vector<OriginId> partnerInputOriginsParam,
    const OriginId outputOriginIdParam,
    const IntervalBound lowerBoundParam,
    const IntervalBound upperBoundParam,
    SliceCacheConfiguration sliceCacheConfigurationParam)
    : lowerBound(lowerBoundParam)
    , upperBound(upperBoundParam)
    , sliceWidth(static_cast<std::uint64_t>(upperBoundParam.getRawValue() - lowerBoundParam.getRawValue()))
    , offsetLow(floorDiv(lowerBoundParam.getRawValue(), static_cast<std::int64_t>(sliceWidth)))
    , offsetHigh(ceilDiv(upperBoundParam.getRawValue(), static_cast<std::int64_t>(sliceWidth)))
    , anchorInputOrigins(std::move(anchorInputOriginsParam))
    , partnerInputOrigins(std::move(partnerInputOriginsParam))
    , outputOriginId(outputOriginIdParam)
    , sliceCacheConfiguration(std::move(sliceCacheConfigurationParam))
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
    /// All three pipelines call start. The worker count is stable across calls (shared worker pool).
    numberOfWorkerThreads.store(pipelineCtx.getNumberOfWorkerThreads(), std::memory_order_release);
}

void IntervalJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    /// Graceful flush runs on the end-of-stream path once both build sides terminate. Nothing to do here.
}

void IntervalJoinOperatorHandler::notifyBufferDone(
    const IntervalJoinBuildSide side, const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    /// Serialize watermark advance + trigger eval to avoid a lost wakeup between the two build sides (see triggerMutex).
    const std::scoped_lock guard{triggerMutex};
    std::ignore = getBuildWatermark(side).updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    tryTriggerAnchors(pipelineCtx);
}

void IntervalJoinOperatorHandler::notifyBufferDoneProbe(const BufferMetaData& bufferMetaData)
{
    std::ignore = watermarkProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);

    const auto probeWatermark = watermarkProbe->getCurrentWatermark();
    anchorStore->garbageCollectSlicesAndWindows(probeWatermark);

    /// Partners outlive their end by max(0, -offsetLow) * W so anchors reaching back still find them.
    const auto partnerShift = static_cast<std::uint64_t>(std::max<std::int64_t>(0, -offsetLow)) * sliceWidth;
    const auto probeWatermarkRaw = probeWatermark.getRawValue();
    if (probeWatermarkRaw > partnerShift)
    {
        partnerStore->garbageCollectSlicesAndWindows(Timestamp{probeWatermarkRaw - partnerShift});
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
        /// Exactly one thread runs the flush so all probe buffers land within one build-termination (before that side's
        /// EOS). Otherwise a straggler buffer lands after the other side's EOS and races teardown.
        if (!terminationFlushed.exchange(true, std::memory_order_acq_rel))
        {
            flushAllOnTermination(pipelineCtx);
        }
    }
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
IntervalJoinOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const
{
    const auto numWorkers = numberOfWorkerThreads.load(std::memory_order_acquire);
    PRECONDITION(numWorkers > 0, "numberOfWorkerThreads not yet set; was IntervalJoinOperatorHandler::start() called?");
    const auto& intervalArgs = dynamic_cast<const CreateNewIntervalJoinSliceArgs&>(args);
    return std::function(
        [numWorkers, bufferProvider = intervalArgs.bufferProvider, tupleSize = intervalArgs.tupleSize](
            SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<IntervalJoinSlice>(*bufferProvider, sliceStart, sliceEnd, numWorkers, tupleSize)}; });
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
    /// Watermarks stay unsigned: timestamps use the full uint64_t range, and narrowing to int64_t turns
    /// everything at or above INT64_MAX negative, which would suppress all triggers.
    const auto partnerShift = static_cast<std::uint64_t>(std::max<std::int64_t>(0, offsetHigh)) * sliceWidth;
    const auto partnerWatermarkRaw = partnerWatermark.getRawValue();
    const auto anchorWatermarkRaw = anchorWatermark.getRawValue();
    if (partnerWatermarkRaw < partnerShift)
    {
        /// Partner side hasn't reached the partner horizon for ANY anchor yet.
        return;
    }
    const auto effectiveWatermarkRaw = std::min(anchorWatermarkRaw, partnerWatermarkRaw - partnerShift);
    if (effectiveWatermarkRaw == Timestamp::INITIAL_VALUE)
    {
        return;
    }
    const Timestamp effectiveWatermark{effectiveWatermarkRaw};

    auto anchors = anchorStore->claimTriggerable(effectiveWatermark);
    if (anchors.empty())
    {
        return;
    }

    assembleAndEmitTriggers(anchors, pipelineCtx);
}

void IntervalJoinOperatorHandler::flushAllOnTermination(PipelineExecutionContext* pipelineCtx)
{
    auto anchors = anchorStore->claimAllNonTriggered();
    if (anchors.empty())
    {
        return;
    }

    assembleAndEmitTriggers(anchors, pipelineCtx);
}

void IntervalJoinOperatorHandler::assembleAndEmitTriggers(
    std::vector<std::shared_ptr<IntervalJoinSlice>>& drivingSlices, PipelineExecutionContext* pipelineCtx)
{
    /// Highest bucket whose end still fits into the unsigned timestamp range.
    const std::uint64_t maxBucketIndex = (std::numeric_limits<std::uint64_t>::max() / sliceWidth) - 1;
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
        auto minPartnerStart = anchor->getSliceStart();

        /// Bucket arithmetic stays unsigned; the signed offset k is applied by partnerBucketIndexFor.
        const auto anchorBucketIndex = anchor->getSliceStart().getRawValue() / sliceWidth;
        for (std::int64_t k = offsetLow; k <= offsetHigh; ++k)
        {
            const auto partnerBucketIndex = partnerBucketIndexFor(anchorBucketIndex, k, maxBucketIndex);
            if (!partnerBucketIndex.has_value())
            {
                continue;
            }
            const SliceEnd partnerEnd{(partnerBucketIndex.value() + 1) * sliceWidth};
            const auto partnerOpt = partnerStore->getSliceBySliceEnd(partnerEnd);
            if (!partnerOpt.has_value())
            {
                continue;
            }
            const auto partnerSlice = std::static_pointer_cast<IntervalJoinSlice>(partnerOpt.value());
            if (partnerSlice->getNumberOfTuples() == 0)
            {
                continue;
            }
            partnerSlice->combinePagedVectors();
            trigger.partnerSliceEnds.at(trigger.partnerCount++) = partnerEnd;
            minPartnerStart = std::min(minPartnerStart, partnerSlice->getSliceStart());
        }
        if (trigger.partnerCount == 0)
        {
            /// Inner join: an anchor with no partners emits nothing; sequence number NOT consumed.
            continue;
        }

        const auto seqRaw = nextProbeSequence.fetch_add(1, std::memory_order_relaxed);
        const SequenceData sequenceData{SequenceNumber{seqRaw}, ChunkNumber{ChunkNumber::INITIAL}, /*lastChunk=*/true};

        /// GC contract: this watermark must not exceed any referenced slice's start. The gap-filling probe
        /// watermark then cannot pass a slice an in-flight probe still reads, so GC leaves it alone.
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
