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
#include <SliceStore/IntervalSliceStoreRef.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <folly/Synchronized.h>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

/// Slice store for one side of the streaming interval join. One instance per side, held concretely
/// by IntervalJoinOperatorHandler.
///
/// Just a flat, time-ordered map of slices: one anchor slice is one output window, partners are
/// looked up by slice end at trigger time. No per-window state machine. Triggering is a sorted
/// prefix scan plus a per-slice atomic CAS. GC drops every slice ended below a watermark the
/// handler supplies.
///
/// Mirrors SlicedWindowStoreInterface by name where the semantics match
/// (getSlicesOrCreate, getSliceBySliceEnd, garbageCollectSlicesAndWindows, deleteState,
/// incrementNumberOfInputPipelines, getWindowSize) but does not inherit it. Its trigger
/// primitive stays as claimTriggerable / claimAllNonTriggered, which return the derived
/// IntervalJoinSlice and own no per-window sequence number, so they cannot match the interface's
/// map-and-sequence signature.
class IntervalSliceStore final
{
public:
    IntervalSliceStore(uint64_t sliceWidth, SliceCacheConfiguration sliceCacheConfiguration);
    ~IntervalSliceStore();

    /// Builds the SliceStoreRef driving this store's build hot path, reusing its sliceCacheConfiguration.
    std::unique_ptr<SliceStoreRef>
    createSliceStoreRef(IntervalSliceStoreRef::DataStructureExtractor extractor, IntervalSliceStoreRef::CreateSlicesFunction creator);

    /// Build hot path (SliceStoreRef cache-miss proxy): look up the slice for `timestamp`, or create
    /// it via the handler-supplied lambda.
    std::vector<std::shared_ptr<Slice>>
    getSlicesOrCreate(Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice);

    /// Exact-key lookup. Used by the probe proxies and the trigger assembly to resolve a slice by end.
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd sliceEnd);

    /// Anchor-side trigger primitive. Atomically claim every not-yet-triggered slice with
    /// sliceEnd <= triggerWatermark, sorted sliceEnd-ascending. Per-slice CAS gives concurrent
    /// triggers on different watermarks disjoint claim sets.
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimTriggerable(Timestamp triggerWatermark);

    /// Anchor-side termination flush. Atomically claim all not-yet-triggered slices regardless of
    /// watermark, when the last input pipeline on this side terminates.
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimAllNonTriggered();

    /// Drop every slice ended below `gcWatermark`. The handler passes the probe watermark for the anchor
    /// store, and `probeWatermark - max(0, -offsetLow) * sliceWidth` for the partner store so partners
    /// outlive the last anchor that could reference them.
    void garbageCollectSlicesAndWindows(Timestamp gcWatermark);

    /// Per-pipeline cache memory allocation, invoked when a build pipeline sets up its slice cache.
    std::span<std::byte>
    allocateSpaceForSliceCache(uint64_t sliceCacheMemorySize, PipelineId pipelineId, AbstractBufferProvider& bufferProvider);

    /// Active build pipelines on this side. Incremented when a build pipeline sets up, decremented when
    /// it terminates. At zero the side is build-done and its termination flush can run.
    void incrementNumberOfInputPipelines();
    [[nodiscard]] bool decrementAndCheckIfLastPipeline() noexcept;

    /// Slice width in ms, named for symmetry with the time-based stores.
    [[nodiscard]] uint64_t getWindowSize() const;

    void deleteState();

private:
    /// Slice width in ms, equal to the interval span `upperBound - lowerBound`. Slices tumble at this
    /// width, so any anchor's partner interval spans a bounded run of consecutive slices.
    const uint64_t sliceWidth;
    SliceAssigner sliceAssigner;
    SliceCacheConfiguration sliceCacheConfiguration;
    folly::Synchronized<std::map<SliceEnd, std::shared_ptr<IntervalJoinSlice>>> slices;
    folly::Synchronized<std::unordered_map<PipelineId, std::unique_ptr<TupleBuffer>>> pipelineIdToSliceCacheStarts;
    std::atomic<uint64_t> numberOfActiveInputPipelines;
};

}
