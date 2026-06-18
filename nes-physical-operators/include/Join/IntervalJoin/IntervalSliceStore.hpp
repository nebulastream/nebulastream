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
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

class IntervalSliceStoreRef;

/// Slice store for one side of the streaming interval join.
///
/// The interval join needs only a flat, time-ordered map of slices: a "window"
/// in the join-output sense is exactly one anchor slice, and the partner slices
/// it joins against are looked up by slice end at trigger time. There is no
/// per-window state machine. The trigger primitive is therefore a sorted prefix
/// scan over the slices plus a per-slice atomic CAS, and garbage collection has
/// two entry points: the anchor side drops slices once they have been triggered
/// and consumed; the partner side drops slices once no anchor can reference them.
///
/// One store instance lives per side inside IntervalJoinOperatorHandler.
///
/// This store deliberately does NOT implement a shared slice-store interface. The interval join's
/// trigger primitive (claimTriggerable below) cannot share a signature with the time-based window
/// stores' getTriggerableWindowSlices: (1) it returns the derived IntervalJoinSlice the handler
/// dereferences directly, not base Slice; (2) it has no per-window grouping / sequence number to
/// report (one anchor slice IS one output window); and (3) it is a destructive atomic claim with a
/// sorted-by-sliceEnd ordering contract. So the interval handler holds this store concretely.
class IntervalSliceStore final
{
public:
    IntervalSliceStore(uint64_t sliceWidth, SliceCacheConfiguration sliceCacheConfiguration);
    ~IntervalSliceStore();

    /// Build hot path, driven by the SliceStoreRef cache-miss proxy: look up the
    /// slice for `timestamp`, or create it via the handler-supplied lambda.
    std::vector<std::shared_ptr<Slice>>
    getSlicesOrCreate(Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice);

    /// Exact-key lookup. Used by the handler when assembling anchor/partner pairs.
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd);

    /// Anchor-side trigger primitive. Atomically claim every slice with
    /// sliceEnd <= triggerWatermark whose triggered flag is still false; return them
    /// in sorted (sliceEnd-ascending) order. Concurrent triggers on different
    /// watermarks each see a disjoint claim set thanks to the per-slice CAS.
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimTriggerable(Timestamp triggerWatermark);

    /// Anchor-side termination flush. Atomically claim all not-yet-triggered
    /// slices regardless of watermark. Called from the build operator's
    /// terminate() proxy when the last input pipeline on this side terminates.
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimAllNonTriggered();

    /// Anchor-side GC: drop slices that have been triggered AND ended below
    /// `gcWatermark` (probe watermark). Called from notifyBufferDoneProbe.
    void garbageCollectTriggered(Timestamp gcWatermark);

    /// Partner-side GC: drop slices whose end is below `gcWatermark`. The handler
    /// computes `gcWatermark = probeWatermark - max(0, -offsetLow) * sliceWidth` so that partner
    /// slices outlive the last possible anchor that could reference them.
    void garbageCollectExpired(Timestamp gcWatermark);

    /// Per-pipeline cache memory allocation, invoked from
    /// IntervalSliceStoreRef::setupSliceStore.
    std::span<std::byte>
    allocateSpaceForSliceCache(uint64_t sliceCacheMemorySize, PipelineId pipelineId, AbstractBufferProvider& bufferProvider);

    /// Tracks how many build pipelines on this side are still active so we know
    /// when the side is fully built. Incremented in setup(), decremented in
    /// terminate(); when it hits zero the side is "build-done" and its
    /// termination flush can run.
    void incrementNumberOfInputPipelines();
    [[nodiscard]] bool decrementAndCheckIfLastPipeline() noexcept;

    /// Slice width in ms; named for symmetry with the time-based stores.
    [[nodiscard]] uint64_t getWindowSize() const;

    void deleteState();

private:
    /// Width of each slice in ms, equal to the interval span `upperBound - lowerBound`.
    /// Slices tumble at this width, so any anchor's partner interval spans at most a
    /// bounded run of consecutive slices.
    const uint64_t sliceWidth;
    SliceAssigner sliceAssigner;
    SliceCacheConfiguration sliceCacheConfiguration;
    folly::Synchronized<std::map<SliceEnd, std::shared_ptr<IntervalJoinSlice>>> slices;
    folly::Synchronized<std::unordered_map<PipelineId, std::unique_ptr<TupleBuffer>>> pipelineIdToSliceCacheStarts;
    std::atomic<uint64_t> numberOfActiveInputPipelines;
};

}
