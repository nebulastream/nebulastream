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
#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>
#include <folly/Synchronized.h>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/RollingAverage.hpp>
#include <HashMapSlice.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// This struct models the information for an aggregation window trigger
/// As we are triggering the probe pipeline by passing a tuple buffer to the probe operator, we assume that the tuple buffer
/// is large enough to store all slices of the window to be triggered.
/// TRIVIALLY DESTRUCTIBLE by design (2c0): this object is placement-new'd into a recycle-only (unpooled) TupleBuffer,
/// and buffer reclamation never runs a C++ destructor on placement-new'd content. It therefore owns NO heap — it carries
/// only raw, non-owning pointers plus the (sequenceNumber, chunkNumber) registry key. The maps it points at are owned by
/// AggregationOperatorHandler's chunkRegistry (final accumulator) and by the slices (inputs, at P=1) — both of which
/// outlive the async probe drain and are freed deterministically (registry: per-chunk release / stop() sweep; slices:
/// emittedKeys + GC). See .planning/dev-eng-cpp-phase2/exploration/EMITTEDWINDOW-LIFETIME-AUDIT.md for why owning heap
/// here would leak on every normal shutdown.
struct EmittedAggregationWindow
{
    EmittedAggregationWindow(
        const WindowInfo windowInfo,
        HashMap* finalHashMapPtr,
        const SequenceNumber sequenceNumber,
        const ChunkNumber chunkNumber,
        const std::vector<HashMap*>& allHashMaps)
        : windowInfo(windowInfo)
        , finalHashMapPtr(finalHashMapPtr)
        , sequenceNumber(sequenceNumber)
        , chunkNumber(chunkNumber)
        , numberOfHashMaps(allHashMaps.size())
    {
        /// Copying the hashmap pointers after this object, hence this + 1
        hashMaps = std::bit_cast<HashMap**>(this + 1);
        std::ranges::copy(allHashMaps, hashMaps);
    }

    WindowInfo windowInfo;
    HashMap* finalHashMapPtr; /// Non-owning: the final hash map is owned by AggregationOperatorHandler::chunkRegistry
    SequenceNumber sequenceNumber; /// Registry key (with chunkNumber) to release the owned maps after the probe drains
    ChunkNumber chunkNumber;
    uint64_t numberOfHashMaps;
    HashMap** hashMaps; /// Non-owning pointers to the hash maps the probe combines (slice-owned at P=1)
};

/// Guards the load-bearing invariant of 2c0: this object is placement-new'd into a recycle-only TupleBuffer whose
/// recycler NEVER runs content destructors. Any non-trivial member added here would silently leak on every normal query
/// teardown (the exact bug EMITTEDWINDOW-LIFETIME-AUDIT.md found and this refactor fixed). Keep it trivially destructible.
static_assert(
    std::is_trivially_destructible_v<EmittedAggregationWindow>,
    "EmittedAggregationWindow must stay trivially destructible: it lives in a recycle-only buffer that never runs a "
    "content destructor. Owned heap belongs in AggregationOperatorHandler::chunkRegistry, not here.");

class AggregationOperatorHandler final : public WindowBasedOperatorHandler
{
public:
    AggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        uint64_t maxNumberOfBuckets,
        bool spillEnabled = false);

    [[nodiscard]] std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
    getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const override;

    /// Is required to not perform the setup again and resolving a race condition to the cleanup state function
    std::atomic<bool> setupAlreadyCalled;
    /// shared_ptr as multiple slices need access to it
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> cleanupStateNautilusFunction;

    /// 2c0 chunk-registry ownership API. Each emitted chunk's handler-owned maps live in chunkRegistry keyed by
    /// (sequenceNumber, chunkNumber). registerChunk is called at emit time (build worker); releaseChunk is called by the
    /// probe (probe worker) once it has finished draining that chunk; stop() sweeps anything left undrained at teardown.
    /// This keeps the emitted buffer trivially destructible so a dropped-undrained buffer cannot leak (see audit doc).
    /// registerChunk returns the raw (non-owning) accumulator pointer to store in EmittedAggregationWindow::finalHashMapPtr.
    HashMap* registerChunk(
        SequenceNumber sequenceNumber,
        ChunkNumber chunkNumber,
        std::unique_ptr<HashMap> finalAccumulator,
        std::vector<std::unique_ptr<HashMap>> inputs = {});
    /// Frees the maps owned for (sequenceNumber, chunkNumber). Idempotent: a release for an absent key is a no-op.
    void releaseChunk(SequenceNumber sequenceNumber, ChunkNumber chunkNumber);

    /// Sweeps any chunks left in the registry (undrained at teardown), then forwards to the base.
    void stop(QueryTerminationType queryTerminationType, PipelineExecutionContext& pipelineExecutionContext) override;

    /// Diagnostic/test accessors for the chunk registry (used by the leak-totality test).
    [[nodiscard]] uint64_t numRegisteredChunks() const { return chunksRegistered.load(std::memory_order_relaxed); }
    [[nodiscard]] uint64_t numReleasedChunks() const { return chunksReleased.load(std::memory_order_relaxed); }
    [[nodiscard]] std::size_t pendingChunkCount() const { return chunkRegistry.rlock()->size(); }

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;
    folly::Synchronized<RollingAverage<uint64_t>> rollingAverageNumberOfKeys;
    uint64_t maxNumberOfBuckets;
    /// When true, getCreateNewSlicesFunction emits SpillableAggregationSlice (paired with a SpillableTimeBasedSliceStore).
    bool spillEnabled;

    /// Owned maps for one emitted chunk. At P=1 `inputs` is empty (the per-worker input maps stay slice-owned); it is
    /// populated from 2d onwards by partition reads. `finalAccumulator` is the map the probe combines into and lowers.
    struct ChunkMaps
    {
        std::vector<std::unique_ptr<HashMap>> inputs;
        std::unique_ptr<HashMap> finalAccumulator;
    };
    folly::Synchronized<std::map<std::pair<SequenceNumber, ChunkNumber>, ChunkMaps>> chunkRegistry;
    std::atomic<uint64_t> chunksRegistered{0};
    std::atomic<uint64_t> chunksReleased{0};
};

}
