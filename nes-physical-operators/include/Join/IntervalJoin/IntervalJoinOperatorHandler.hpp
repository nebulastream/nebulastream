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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
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
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// Build input a record arrived on. Anchor = left, Partner = right. The roles drive trigger emission.
enum class IntervalJoinBuildSide : std::uint8_t
{
    Anchor,
    Partner
};

/// One trigger buffer per anchor slice, carrying up to MAX_PARTNERS partner slice ends.
/// MAX_PARTNERS=3: with slice width W, an interval of length W overlaps at most 3 consecutive width-W buckets.
struct EmittedIntervalJoinWindowTrigger
{
    static constexpr std::size_t MAX_PARTNERS = 3;

    /// Driving anchor-store slice end; its partners live in the partner store.
    SliceEnd anchorSliceEnd{0};
    std::array<SliceEnd, MAX_PARTNERS> partnerSliceEnds = {SliceEnd{0}, SliceEnd{0}, SliceEnd{0}};
    std::uint8_t partnerCount = 0;
    WindowInfo windowInfo{0, 0};
};

/// Operator handler for the streaming interval join. Extends OperatorHandler, not
/// WindowBasedOperatorHandler (which assumes one store + one combined build watermark).
///
/// Holds per-side stores + watermarks. An anchor triggers once both build watermarks pass its
/// partner horizon. A partner matches an anchor at tA iff tP in [tA+lowerBound, tA+upperBound].
///
///   anchor slice  S_A^i           [iStart, iEnd)
///   partner slices in buckets      offsetLow..offsetHigh   ->  one probe buffer per anchor
class IntervalJoinOperatorHandler final : public OperatorHandler
{
public:
    IntervalJoinOperatorHandler(
        std::vector<OriginId> anchorInputOrigins,
        std::vector<OriginId> partnerInputOrigins,
        OriginId outputOriginId,
        IntervalBound lowerBound,
        IntervalBound upperBound,
        SliceCacheConfiguration sliceCacheConfiguration);

    void start(PipelineExecutionContext& ctx, std::uint32_t localStateVariableId) override;
    void stop(QueryTerminationType type, PipelineExecutionContext& ctx) override;

    /// Advances the given side's build watermark and re-runs the anchor-trigger loop (either side's
    /// advance may unblock anchors).
    void notifyBufferDone(IntervalJoinBuildSide side, const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx);

    /// Advances the probe watermark, then garbage-collects both stores (anchor at probeWatermark,
    /// partner at probeWatermark shifted by max(0, -offsetLow) * W).
    void notifyBufferDoneProbe(const BufferMetaData& bufferMetaData);

    /// Marks the side done. Runs the end-of-stream flush once both sides are done.
    void onSideBuildTerminated(IntervalJoinBuildSide side, PipelineExecutionContext* pipelineCtx);

    /// Slice-creation closure handed to the store for cache misses. Captures numberOfWorkerThreads.
    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const;

    [[nodiscard]] IntervalSliceStore& getAnchorStore() noexcept;
    [[nodiscard]] IntervalSliceStore& getPartnerStore() noexcept;

    [[nodiscard]] IntervalSliceStore& getStore(IntervalJoinBuildSide side) noexcept;

    [[nodiscard]] IntervalBound getLowerBound() const noexcept { return lowerBound; }

    [[nodiscard]] IntervalBound getUpperBound() const noexcept { return upperBound; }

    [[nodiscard]] std::uint64_t getSliceWidth() const noexcept { return sliceWidth; }

    [[nodiscard]] std::uint64_t getNumberOfWorkerThreads() const noexcept { return numberOfWorkerThreads; }

    [[nodiscard]] OriginId getOutputOriginId() const noexcept { return outputOriginId; }

private:
    [[nodiscard]] MultiOriginWatermarkProcessor& getBuildWatermark(IntervalJoinBuildSide side) noexcept;

    /// Claims newly-triggerable anchor slices and emits one probe buffer each.
    void tryTriggerAnchors(PipelineExecutionContext* pipelineCtx);

    /// End-of-stream flush: claims every remaining anchor regardless of watermark.
    void flushAllOnTermination(PipelineExecutionContext* pipelineCtx);

    /// Shared assembly loop behind both emit paths. For each non-empty anchor slice, gathers non-empty partner
    /// slices in [offsetLow, offsetHigh] from the partner store and emits one probe buffer (skipping anchors with
    /// no partners).
    void assembleAndEmitTriggers(std::vector<std::shared_ptr<IntervalJoinSlice>>& drivingSlices, PipelineExecutionContext* pipelineCtx);

    /// Serializes one trigger struct into a buffer (numberOfTuples = 1) and dispatches it to the probe.
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

    std::unique_ptr<IntervalSliceStore> anchorStore;
    std::unique_ptr<IntervalSliceStore> partnerStore;

    /// Per-side build watermarks (anchor, partner) plus the probe/output watermark from which GC thresholds derive.
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkAnchor;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkPartner;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProbe;

    /// Serializes each side's "advance my build watermark + evaluate triggers" step so the second side in sees both
    /// fresh watermarks. Otherwise a concurrently-triggerable anchor is claimed by neither (lost wakeup).
    std::mutex triggerMutex;

    std::atomic<SequenceNumber::Underlying> nextProbeSequence;
    std::atomic<std::uint64_t> numberOfWorkerThreads;

    /// Set when each side's build finishes (concurrent pipeline threads). Atomic so the "both done" check is race-free.
    std::atomic<bool> anchorBuildDone;
    std::atomic<bool> partnerBuildDone;
    /// Guards the end-of-stream flush to exactly one thread, so its probe buffers cannot land after the other
    /// side's EOS and race teardown.
    std::atomic<bool> terminationFlushed;
};

}
