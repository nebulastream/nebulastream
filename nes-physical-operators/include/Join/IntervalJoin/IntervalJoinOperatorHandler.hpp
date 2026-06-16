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
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// Trigger payload emitted by IntervalJoinOperatorHandler per anchor slice.
///
/// We diverge from NLJ's per-pair shape: each anchor produces ONE buffer
/// containing up to MAX_PARTNERS partner slice ends. The upper bound is 3 for
/// `W = U - L` (proof: an interval of length W overlaps at most 3 consecutive
/// width-W buckets). This halves task count vs per-pair emit and removes the
/// chunking dance that would otherwise be needed for the probe-WM processor.
struct EmittedIntervalJoinWindowTrigger
{
    static constexpr std::size_t MAX_PARTNERS = 3;

    /// Anchor slice end. For the normal (left-anchored) pass this is a left-store slice and the partners
    /// are right-store slices. For a right-null-fill pass (rightNullFillPass = true) the roles are swapped:
    /// the anchor is a right-store slice and the partners are left-store slices.
    SliceEnd leftSliceEnd{0};
    SliceEnd partnerSliceEnds[MAX_PARTNERS] = {SliceEnd{0}, SliceEnd{0}, SliceEnd{0}};
    std::uint8_t partnerCount = 0;
    WindowInfo windowInfo{0, 0};
    /// When true the probe runs a right-anchored null-fill pass (RIGHT/FULL outer): for each right anchor
    /// tuple with no matching left partner it emits a row with the left-side fields set to null.
    bool rightNullFillPass = false;
};

/// Operator handler for the streaming interval join.
///
/// Holds two IntervalSliceStores (left + right), three watermark processors
/// (left build, right build, probe), and the bound arithmetic that drives
/// trigger emission. Extends OperatorHandler directly — NOT
/// WindowBasedOperatorHandler — because the base's "one store + one combined
/// build WM" assumption doesn't fit per-side stores with per-side watermarks.
class IntervalJoinOperatorHandler final : public OperatorHandler
{
public:
    IntervalJoinOperatorHandler(
        std::vector<OriginId> leftInputOrigins,
        std::vector<OriginId> rightInputOrigins,
        OriginId outputOriginId,
        std::int64_t lowerBound,
        std::int64_t upperBound,
        SliceCacheConfiguration sliceCacheConfiguration,
        bool emitLeftNullFill = false,
        bool emitRightNullFill = false);

    void start(PipelineExecutionContext& ctx, std::uint32_t localStateVariableId) override;
    void stop(QueryTerminationType type, PipelineExecutionContext& ctx) override;

    /// Build-side notifications. Called from IntervalJoinBuildPhysicalOperator::close
    /// via the side-aware proxy. Each advances its own build watermark and re-runs
    /// the anchor-trigger loop, since either side's WM advance may unblock anchors.
    void notifyBufferDoneLeft(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);
    void notifyBufferDoneRight(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /// Probe-side notification. Advances the probe watermark, then GCs both
    /// stores (left at probeWm, right at probeWm shifted by max(0, -offsetLow) * W).
    void notifyBufferDoneProbe(const BufferMetaData& bufferMetaData);

    /// Called from the build operator's terminate() proxy AFTER the side's
    /// slice-store reports its last input pipeline. Marks the side done; when
    /// both sides are done, runs the end-of-stream flush.
    void onSideBuildTerminated(JoinBuildSideType side, PipelineExecutionContext* pipelineCtx);

    /// Lambda factory used by IntervalSliceStoreRef's cache-miss proxy. Returns
    /// a closure that captures numberOfWorkerThreads by value (set in start()).
    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const;

    [[nodiscard]] IntervalSliceStore& getLeftStore() noexcept;
    [[nodiscard]] IntervalSliceStore& getRightStore() noexcept;

    [[nodiscard]] std::int64_t getLowerBound() const noexcept { return lowerBound; }

    [[nodiscard]] std::int64_t getUpperBound() const noexcept { return upperBound; }

    [[nodiscard]] std::uint64_t getSliceWidth() const noexcept { return widthW; }

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

    /// RIGHT/FULL outer end-of-stream pass. Anchors on the right store and looks up left partner slices
    /// (symmetric to the left-anchored trigger). Emits one right-null-fill buffer per non-empty right
    /// anchor so the probe can null-fill right tuples that matched no left tuple. Runs at termination,
    /// when GC has been suppressed so every left partner is still present.
    void flushRightNullFill(PipelineExecutionContext* pipelineCtx);

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
    const std::uint64_t widthW;
    const std::int64_t offsetLow;
    const std::int64_t offsetHigh;

    const std::vector<OriginId> leftInputOrigins;
    const std::vector<OriginId> rightInputOrigins;
    const OriginId outputOriginId;
    const SliceCacheConfiguration sliceCacheConfiguration;
    /// LEFT/FULL outer interval join: emit anchor buffers even when an anchor has no non-empty partner
    /// slices, so the probe can null-fill the unmatched left tuples.
    const bool emitLeftNullFill;
    /// RIGHT/FULL outer interval join: run a right-anchored null-fill pass at termination, and suppress
    /// GC meanwhile so left partner slices survive for that pass.
    const bool emitRightNullFill;

    // todo please rename left and right and use anchor and other name we came up with. Do this for all left and right in the IntervalJoin physical operators (build, probe), operator handler, and the
    std::unique_ptr<IntervalSliceStore> leftStore;
    std::unique_ptr<IntervalSliceStore> rightStore;

    std::unique_ptr<MultiOriginWatermarkProcessor> wmLeft;
    std::unique_ptr<MultiOriginWatermarkProcessor> wmRight;
    std::unique_ptr<MultiOriginWatermarkProcessor> wmProbe;

    std::atomic<SequenceNumber::Underlying> nextProbeSequence;
    std::atomic<std::uint64_t> numberOfWorkerThreads;
    std::atomic<bool> leftBuildDone;
    std::atomic<bool> rightBuildDone;
};

}
