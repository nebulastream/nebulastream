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
#include <Time/IntervalBound.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// Which build input a record arrived on. The interval join labels its inputs anchor (left) and
/// partner (right) rather than reusing the shared JoinBuildSideType, whose Left/Right names would
/// obscure the anchor/partner roles that drive trigger emission.
enum class IntervalJoinBuildSide : std::uint8_t
{
    Anchor,
    Partner
};

/// Trigger payload emitted by IntervalJoinOperatorHandler per anchor slice.
///
/// Each anchor slice produces ONE buffer carrying up to MAX_PARTNERS partner slice ends. The bound
/// is 3 because, with slice width `W = upperBound - lowerBound`, an interval of length W overlaps at
/// most 3 consecutive width-W buckets. Emitting one buffer per anchor (instead of one per
/// anchor/partner pair) keeps the task count low and avoids chunking the probe-side watermark.
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
/// build watermark" assumption doesn't fit per-side stores with per-side watermarks.
class IntervalJoinOperatorHandler final : public OperatorHandler
{
public:
    IntervalJoinOperatorHandler(
        std::vector<OriginId> anchorInputOrigins,
        std::vector<OriginId> partnerInputOrigins,
        OriginId outputOriginId,
        IntervalBound lowerBound,
        IntervalBound upperBound,
        SliceCacheConfiguration sliceCacheConfiguration,
        bool emitAnchorNullFill = false,
        bool emitPartnerNullFill = false);

    void start(PipelineExecutionContext& ctx, std::uint32_t localStateVariableId) override;
    void stop(QueryTerminationType type, PipelineExecutionContext& ctx) override;

    /// Build-side notification. Called from IntervalJoinBuildPhysicalOperator::close via the
    /// side-aware proxy. Advances the given side's build watermark and re-runs the anchor-trigger
    /// loop, since either side's watermark advance may unblock anchors.
    void notifyBufferDone(IntervalJoinBuildSide side, const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /// Probe-side notification. Advances the probe watermark, then garbage-collects both
    /// stores (anchor at probeWatermark, partner at probeWatermark shifted by max(0, -offsetLow) * W).
    void notifyBufferDoneProbe(const BufferMetaData& bufferMetaData);

    /// Called from the build operator's terminate() proxy AFTER the side's
    /// slice-store reports its last input pipeline. Marks the side done; when
    /// both sides are done, runs the end-of-stream flush.
    void onSideBuildTerminated(IntervalJoinBuildSide side, PipelineExecutionContext* pipelineCtx);

    /// Lambda factory used by IntervalSliceStoreRef's cache-miss proxy. Returns
    /// a closure that captures numberOfWorkerThreads by value (set in start()).
    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const;

    [[nodiscard]] IntervalSliceStore& getAnchorStore() noexcept;
    [[nodiscard]] IntervalSliceStore& getPartnerStore() noexcept;

    /// Side-dispatched store accessor used by the build operator's side-parameterized proxies.
    [[nodiscard]] IntervalSliceStore& getStore(IntervalJoinBuildSide side) noexcept;

    [[nodiscard]] IntervalBound getLowerBound() const noexcept { return lowerBound; }

    [[nodiscard]] IntervalBound getUpperBound() const noexcept { return upperBound; }

    [[nodiscard]] std::uint64_t getSliceWidth() const noexcept { return sliceWidth; }

    [[nodiscard]] std::uint64_t getNumberOfWorkerThreads() const noexcept { return numberOfWorkerThreads; }

    [[nodiscard]] OriginId getOutputOriginId() const noexcept { return outputOriginId; }

private:
    /// Side-dispatched build-watermark accessor (anchor vs partner), mirroring getStore.
    [[nodiscard]] MultiOriginWatermarkProcessor& getBuildWatermark(IntervalJoinBuildSide side) noexcept;

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
    /// Runs at termination, when garbage collection has been suppressed so every anchor-side partner is still present.
    void flushPartnerNullFill(PipelineExecutionContext* pipelineCtx);

    /// Shared trigger-assembly loop behind all three emit paths (steady-state anchor trigger, the
    /// end-of-stream anchor flush, and the partner-anchored null-fill pass). For each non-empty driving
    /// slice it gathers the non-empty partner slices in [offsetLow, offsetHigh] from `partnerStore` and
    /// emits one probe buffer. In the partner-null-fill pass (`partnerNullFillPass`) a buffer is emitted
    /// even with zero partners so every unmatched partner tuple gets null-filled; otherwise emission is
    /// skipped unless there is at least one partner or emitAnchorNullFill is set.
    void assembleAndEmitTriggers(
        std::vector<std::shared_ptr<IntervalJoinSlice>>& drivingSlices,
        IntervalSliceStore& partnerStore,
        std::int64_t offsetLowParam,
        std::int64_t offsetHighParam,
        bool partnerNullFillPass,
        PipelineExecutionContext* pipelineCtx);

    /// Serializes a single trigger struct into a buffer (one buffer per anchor, carrying the partner
    /// slice-end array; numberOfTuples = 1) and dispatches it to the probe.
    void emitProbeBuffer(
        const EmittedIntervalJoinWindowTrigger& trigger,
        const SequenceData& sequenceData,
        Timestamp watermark,
        PipelineExecutionContext* pipelineCtx) const;

    const IntervalBound lowerBound;
    const IntervalBound upperBound;
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
    /// RIGHT/FULL outer interval join: run a partner-anchored null-fill pass at termination, and suppress
    /// garbage collection meanwhile so anchor-side partner slices survive for that pass.
    const bool emitPartnerNullFill;

    std::unique_ptr<IntervalSliceStore> anchorStore;
    std::unique_ptr<IntervalSliceStore> partnerStore;

    /// Per-side watermark processors. watermarkAnchor and watermarkPartner track how far the anchor (left) and
    /// partner (right) build inputs have advanced; watermarkProbe tracks the probe (output) side. An anchor
    /// slice only triggers once both build watermarks have passed its partner horizon, and the
    /// garbage-collection thresholds are derived from watermarkProbe.
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkAnchor;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkPartner;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProbe;

    std::atomic<SequenceNumber::Underlying> nextProbeSequence;
    std::atomic<std::uint64_t> numberOfWorkerThreads;

    /// Set from each build side's terminate(), which run concurrently on independent pipeline threads;
    /// atomic so the "both sides done" check that fires the end-of-stream flush stays race-free.
    std::atomic<bool> anchorBuildDone;
    std::atomic<bool> partnerBuildDone;
};

}
