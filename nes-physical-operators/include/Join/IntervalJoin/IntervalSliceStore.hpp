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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

class IntervalSliceStoreRef;

/// Leaner mirror of DefaultTimeBasedSliceStore for the streaming interval join.
///
// todo do not mention here the DefaultTimeBasedSliceStore. just write what the interval join requires 
/// DefaultTimeBasedSliceStore carries a windows map plus a per-window state
/// machine to support tumbling/sliding aggregations and joins. IntervalJoin
/// doesn't need that — a "window" in the join-output sense is exactly one
/// anchor slice, and partner slices are looked up at trigger time. So this
/// store drops the windows map, replaces the trigger primitive with a sorted
/// prefix scan plus per-slice atomic CAS, and exposes two GC entry points
/// (one for the left store, one for the right) instead of the combined one
/// the default store offers.
///
/// One store instance lives per side inside IntervalJoinOperatorHandler.
class IntervalSliceStore final : public WindowSlicesStoreInterface
{
public:
    IntervalSliceStore(uint64_t widthW, SliceCacheConfiguration sliceCacheConfiguration);
    ~IntervalSliceStore() override;

    /// Build hot path: look up the slice for `timestamp`, or create it via the
    /// handler-supplied lambda. Matches DefaultTimeBasedSliceStore's signature
    /// so the existing SliceStoreRef cache-miss proxy plugs in unchanged.
    std::vector<std::shared_ptr<Slice>> getSlicesOrCreate(
        Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice) override;

    /// Exact-key lookup. Used by the handler when assembling anchor/partner pairs.
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd) override;

    /// LEFT-store trigger primitive. Atomically claim every slice with
    /// sliceEnd <= triggerWm whose triggered flag is still false; return them
    /// in sorted (sliceEnd-ascending) order. Concurrent triggers on different
    /// watermarks each see a disjoint claim set thanks to the per-slice CAS.
    // todo combine claimTriggerable with the existing WindowSlicesStoreInterface. Come up with a changed interface that works for both, DefaultTimeBasedSliceStore and IntervalSliceStore
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimTriggerable(Timestamp triggerWm);

    /// LEFT-store termination flush. Atomically claim all not-yet-triggered
    /// slices regardless of watermark. Called from the build operator's
    /// terminate() proxy when the last input pipeline on this side terminates.
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimAllNonTriggered();

    /// LEFT-store GC: drop slices that have been triggered AND ended below
    /// `gcWm` (probe watermark). Called from notifyBufferDoneProbe.
    void garbageCollectTriggered(Timestamp gcWm);

    // todo do not use left and right. come up with anchor and another term.
    /// RIGHT-store GC: drop slices whose end is below `gcWm`. The handler
    /// computes `gcWm = probeWm - max(0, -offsetLow) * widthW` so that right
    /// slices outlive the last possible anchor that could reference them.
    void garbageCollectExpired(Timestamp gcWm);

    /// Per-pipeline cache memory allocation. Mirrors DefaultTimeBasedSliceStore
    /// so IntervalSliceStoreRef::setupSliceStore can call this unchanged.
    std::span<std::byte>
    allocateSpaceForSliceCache(uint64_t sliceCacheMemorySize, PipelineId pipelineId, AbstractBufferProvider& bufferProvider);

    // todo do not mention here NLJ. just state why we need this
    /// Pipeline-count idiom matching NLJ's: tracks how many build pipelines on
    /// this side are still active. Incremented in setup(), decremented in
    /// terminate(); when it hits zero the side is "build-done".
    void incrementNumberOfInputPipelines() override;
    [[nodiscard]] bool decrementAndCheckIfLastPipeline() noexcept;

    [[nodiscard]] uint64_t getWindowSize() const override;

    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
    getTriggerableWindowSlices(Timestamp globalWatermark) override;
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override;
    void garbageCollectSlicesAndWindows(Timestamp newGlobalWaterMark) override;
    void deleteState() override;

private:
    // todo please write out what W stands for in widthW
    const uint64_t widthW;
    SliceAssigner sliceAssigner;
    SliceCacheConfiguration sliceCacheConfiguration;
    folly::Synchronized<std::map<SliceEnd, std::shared_ptr<IntervalJoinSlice>>> slices;
    folly::Synchronized<std::unordered_map<PipelineId, std::unique_ptr<TupleBuffer>>> pipelineIdToSliceCacheStarts;
    std::atomic<uint64_t> numberOfActiveInputPipelines;
};

}
