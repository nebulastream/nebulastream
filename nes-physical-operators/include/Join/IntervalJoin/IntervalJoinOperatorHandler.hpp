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

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalSliceStore.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/TimeBasedWindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// Trigger payload emitted by IntervalJoinOperatorHandler per anchor slice.
///
// todo do not mention NLJ here. Simply start with only stuff from IntervalJoin
/// We diverge from NLJ's per-pair shape: each anchor produces ONE buffer
/// containing up to MAX_PARTNERS partner slice ends. The upper bound is 3 for
/// `W = U - L` (proof: an interval of length W overlaps at most 3 consecutive
/// width-W buckets). This halves task count vs per-pair emit and removes the
/// chunking dance that would otherwise be needed for the probe-WM processor.
struct EmittedIntervalJoinWindowTrigger
{
    static constexpr std::size_t MAX_PARTNERS = 3;

    /// Driving slice end. For the normal pass this is an anchor-store slice and the partners
    /// are partner-store slices. For a null-fill pass (partnerNullFillPass = true) the roles are
    /// swapped: the driving slice is a partner-store slice and its partners are anchor-store slices.
    SliceEnd anchorSliceEnd{0};
    SliceEnd partnerSliceEnds[MAX_PARTNERS] = {SliceEnd{0}, SliceEnd{0}, SliceEnd{0}};
    std::uint8_t partnerCount = 0;
    WindowInfo windowInfo{0, 0};
    /// When true the probe runs a partner-anchored null-fill pass (RIGHT/FULL outer): for each partner
    /// anchor tuple with no matching anchor-side partner it emits a row with the anchor-side fields set to null.
    bool partnerNullFillPass = false;
};

/// Operator handler for the streaming interval join.
///
/// Holds two IntervalSliceStores (anchor + partner), three watermark processors
/// (anchor build, partner build, probe), and the bound arithmetic that drives
/// trigger emission. Extends OperatorHandler directly — NOT
/// WindowBasedOperatorHandler — because the base's "one store + one combined
/// build WM" assumption doesn't fit per-side stores with per-side watermarks.
class IntervalJoinOperatorHandler final : public OperatorHandler
{
public:
    IntervalJoinOperatorHandler(
        std::vector<OriginId> anchorInputOrigins,
        std::vector<OriginId> partnerInputOrigins,
        OriginId outputOriginId,
        std::int64_t lowerBound,
        std::int64_t upperBound,
        SliceCacheConfiguration sliceCacheConfiguration,
        bool emitAnchorNullFill = false,
        bool emitPartnerNullFill = false);

    void start(PipelineExecutionContext& ctx, std::uint32_t localStateVariableId) override;
    void stop(QueryTerminationType type, PipelineExecutionContext& ctx) override;

    /// Build-side notifications. Called from IntervalJoinBuildPhysicalOperator::close
    /// via the side-aware proxy. Each advances its own build watermark and re-runs
    /// the anchor-trigger loop, since either side's WM advance may unblock anchors.
    void notifyBufferDoneAnchor(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);
    void notifyBufferDonePartner(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /// Probe-side notification. Advances the probe watermark, then GCs both
    /// stores (anchor at probeWm, partner at probeWm shifted by max(0, -offsetLow) * W).
    void notifyBufferDoneProbe(const BufferMetaData& bufferMetaData);

    /// Called from the build operator's terminate() proxy AFTER the side's
    /// slice-store reports its last input pipeline. Marks the side done; when
    /// both sides are done, runs the end-of-stream flush.
    void onSideBuildTerminated(JoinBuildSideType side, PipelineExecutionContext* pipelineCtx);

    /// Lambda factory used by IntervalSliceStoreRef's cache-miss proxy. Returns
    /// a closure that captures numberOfWorkerThreads by value (set in start()).
    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const;

    [[nodiscard]] IntervalSliceStore& getAnchorStore() noexcept;
    [[nodiscard]] IntervalSliceStore& getPartnerStore() noexcept;

    [[nodiscard]] std::int64_t getLowerBound() const noexcept { return lowerBound; }

    [[nodiscard]] std::int64_t getUpperBound() const noexcept { return upperBound; }

    [[nodiscard]] std::uint64_t getSliceWidth() const noexcept { return sliceWidth; }

    [[nodiscard]] std::uint64_t getNumberOfWorkerThreads() const noexcept { return numberOfWorkerThreads; }

    [[nodiscard]] OriginId getOutputOriginId() const noexcept { return outputOriginId; }

private:
    /// Heart of the operator: claims newly-triggerable anchor slices, looks up
    /// non-empty partner slices for each, and emits one probe buffer per anchor.
    void tryTriggerAnchors(PipelineExecutionContext* pipelineCtx);

    /// End-of-stream flush. Called from onSideBuildTerminated once both sides
    /// have reported their last input pipeline. Claims every remaining anchor
    /// regardless of watermark.
    void flushAllOnTermination(PipelineExecutionContext* pipelineCtx);

    /// RIGHT/FULL outer end-of-stream pass. Anchors on the partner store and looks up anchor-side partner
    /// slices (symmetric to the normal anchor-driven trigger). Emits one partner-null-fill buffer per
    /// non-empty partner anchor so the probe can null-fill partner tuples that matched no anchor-side tuple.
    /// Runs at termination, when GC has been suppressed so every anchor-side partner is still present.
    void flushPartnerNullFill(PipelineExecutionContext* pipelineCtx);

    /// Serializes a single trigger struct into a buffer and dispatches it.
    /// Mirrors NLJOperatorHandler::emitSlicesToProbe; differs in payload shape
    /// (one buffer per anchor; partner array; numberOfTuples = 1).
    void emitProbeBuffer(
        const EmittedIntervalJoinWindowTrigger& trigger,
        const SequenceData& sequenceData,
        Timestamp watermark,
        PipelineExecutionContext* pipelineCtx) const;

    const std::int64_t lowerBound;
    const std::int64_t upperBound;
    const std::uint64_t sliceWidth;
    const std::int64_t offsetLow;
    const std::int64_t offsetHigh;

    const std::vector<OriginId> anchorInputOrigins;
    const std::vector<OriginId> partnerInputOrigins;
    const OriginId outputOriginId;
    const SliceCacheConfiguration sliceCacheConfiguration;
    /// LEFT/FULL outer interval join: emit anchor buffers even when an anchor has no non-empty partner
    /// slices, so the probe can null-fill the unmatched anchor tuples.
    const bool emitAnchorNullFill;
    // todo write out GC to garbage collection
    /// RIGHT/FULL outer interval join: run a partner-anchored null-fill pass at termination, and suppress
    /// GC meanwhile so anchor-side partner slices survive for that pass.
    const bool emitPartnerNullFill;

    std::unique_ptr<IntervalSliceStore> anchorStore;
    std::unique_ptr<IntervalSliceStore> partnerStore;

    // todo write out watermark for wmAnchor, wmPartner, and wmProbe
    std::unique_ptr<MultiOriginWatermarkProcessor> wmAnchor;
    std::unique_ptr<MultiOriginWatermarkProcessor> wmPartner;
    std::unique_ptr<MultiOriginWatermarkProcessor> wmProbe;

    std::atomic<SequenceNumber::Underlying> nextProbeSequence;
    std::atomic<std::uint64_t> numberOfWorkerThreads;

    // todo write one line comment why we need these atomics
    std::atomic<bool> anchorBuildDone;
    std::atomic<bool> partnerBuildDone;
};

}
