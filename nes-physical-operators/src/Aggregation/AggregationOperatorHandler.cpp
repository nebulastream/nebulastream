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
#include <Aggregation/AggregationOperatorHandler.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/SpillableAggregationSlice.hpp>
#include <HashMapSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillableTimeBasedSliceStore.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

AggregationOperatorHandler::AggregationOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets,
    const bool spillEnabled)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , setupAlreadyCalled(false)
    , rollingAverageNumberOfKeys(RollingAverage<uint64_t>{100})
    , maxNumberOfBuckets(maxNumberOfBuckets)
    , spillEnabled(spillEnabled)
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
AggregationOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    auto newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    newHashMapArgs.numberOfBuckets = std::clamp(rollingAverageNumberOfKeys.rlock()->getAverage(), 1UL, maxNumberOfBuckets);
    return std::function(
        [outputOriginId = outputOriginId,
         numberOfWorkerThreads = numberOfWorkerThreads,
         copyOfNewHashMapArgs = newHashMapArgs,
         spillEnabled = spillEnabled](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new aggregation slice with for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            if (spillEnabled)
            {
                return {std::make_shared<SpillableAggregationSlice>(sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads)};
            }
            return {std::make_shared<AggregationSlice>(sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads)};
        });
}

void AggregationOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        /// Getting all hashmaps for each slice that has at least one tuple
        std::unique_ptr<ChainedHashMap> finalHashMap;
        std::vector<HashMap*> allHashMaps;
        uint64_t totalNumberOfTuples = 0;
        for (const auto& slice : allSlices)
        {
            /// Route-B seam: operate on the polymorphic HashMapSlice so this works for both AggregationSlice and
            /// SpillableAggregationSlice. The store has already unspilled any spilled slice on the read path (D3).
            const auto hashMapSlice = std::dynamic_pointer_cast<HashMapSlice>(slice);
            for (uint64_t hashMapIdx = 0; hashMapIdx < hashMapSlice->getNumberOfHashMaps(); ++hashMapIdx)
            {
                if (auto* hashMap = hashMapSlice->getHashMapPtr(WorkerThreadId(hashMapIdx));
                    (hashMap != nullptr) and hashMap->getNumberOfTuples() > 0)
                {
                    /// As the hashmap has one value per key, we can use the number of tuples for the number of keys
                    rollingAverageNumberOfKeys.wlock()->add(hashMap->getNumberOfTuples());

                    /// We store here the raw pointer, as we need the raw pointers to operate over them in the AggregationProbe
                    allHashMaps.emplace_back(hashMap);
                    totalNumberOfTuples += hashMap->getNumberOfTuples();
                    if (not finalHashMap)
                    {
                        finalHashMap = ChainedHashMap::createNewMapWithSameConfiguration(*dynamic_cast<ChainedHashMap*>(hashMap));
                    }
                }
            }
        }

        /// P==1 (and the in-memory store / steady-state path): emit exactly one chunk per window, INITIAL + lastChunk=true,
        /// with EMPTY inputMaps — the per-worker input maps stay slice-owned (byte-identical to the pre-2d emit). The
        /// shared emitChunk helper does the registry insert + buffer alloc + placement-new + emit.
        emitChunk(
            windowInfo.windowInfo,
            windowInfo.sequenceNumber,
            ChunkNumber(ChunkNumber::INITIAL),
            /*lastChunk=*/true,
            std::move(finalHashMap),
            /*inputMaps=*/{},
            allHashMaps,
            totalNumberOfTuples,
            pipelineCtx);
    }
}

void AggregationOperatorHandler::emitChunk(
    const WindowInfo& windowInfo,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    std::unique_ptr<HashMap> finalAccumulator,
    std::vector<std::unique_ptr<HashMap>> inputMaps,
    const std::vector<HashMap*>& allHashMapPtrs,
    const uint64_t numberOfTuples,
    PipelineExecutionContext* pipelineCtx)
{
    /// M3: a null accumulator with non-empty inputMaps is an inconsistent call — registerChunk only runs when the
    /// accumulator is non-null (an empty/null-accumulator chunk registers nothing), so non-empty inputMaps would then be
    /// dropped on return, leaking them. Both legitimate callers uphold this: P=1 passes empty inputMaps; the P>1 path
    /// only builds inputMaps for a non-empty partition, which always has a non-null accumulator. Guard future callers.
    INVARIANT(
        finalAccumulator != nullptr || inputMaps.empty(),
        "emitChunk called with a null final accumulator but {} non-empty input map(s) — they would leak",
        inputMaps.size());

    /// We need a buffer that is large enough to store:
    /// - all pointers to all hashmaps of the window to be triggered
    /// - a new hashmap for the probe operator, so that we are not overwriting the thread local hashmaps
    /// - size of EmittedAggregationWindow
    const auto neededBufferSize = sizeof(EmittedAggregationWindow) + (allHashMapPtrs.size() * sizeof(HashMap*));
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the aggregation window trigger were requested", neededBufferSize);
    }
    auto tupleBuffer = tupleBufferVal.value();

    /// It might be that the buffer is not zeroed out.
    std::ranges::fill(tupleBuffer.getAvailableMemoryArea(), std::byte{0});

    /// As we are here "emitting" a buffer, we have to set the originId, the seq number, the watermark and the "number of tuples".
    /// The watermark cannot be the slice end as some buffers might be still waiting to get processed. 2d: chunkNumber,
    /// lastChunk and numberOfTuples come from the args so the P>1 path can emit K contiguous chunks (the K-th lastChunk).
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(sequenceNumber);
    tupleBuffer.setChunkNumber(chunkNumber);
    tupleBuffer.setLastChunk(lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(numberOfTuples);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));

    /// 2c0/2d: the final accumulator AND (from 2d) the freshly-materialised per-worker partition input maps are owned by
    /// the handler's chunkRegistry (NOT the recycle-only emit buffer), keyed by (sequenceNumber, chunkNumber); they are
    /// freed when the probe finishes draining this chunk (releaseChunk) or swept by stop() at teardown. The emitted
    /// buffer carries only raw, non-owning pointers, which keeps it trivially destructible so a dropped-undrained buffer
    /// cannot leak. A chunk with no non-empty maps has a null accumulator and registers nothing (matches P=1 empty window).
    HashMap* finalHashMapPtr = nullptr;
    if (finalAccumulator)
    {
        finalHashMapPtr = registerChunk(sequenceNumber, chunkNumber, std::move(finalAccumulator), std::move(inputMaps));
    }

    /// Writing all necessary information for the aggregation probe to the buffer via the placement new constructor
    auto tmp = tupleBuffer.getAvailableMemoryArea();
    new (tmp.data()) EmittedAggregationWindow{windowInfo, finalHashMapPtr, sequenceNumber, chunkNumber, allHashMapPtrs};

    /// Dispatching the buffer to the probe operator via the task queue.
    pipelineCtx->emitBuffer(tupleBuffer);
    NES_TRACE(
        "Emitted window {}-{} chunk {} (lastChunk={}, tuples={}) with watermarkTs {} sequenceNumber {} originId {}",
        windowInfo.windowStart,
        windowInfo.windowEnd,
        chunkNumber,
        lastChunk,
        numberOfTuples,
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceNumber(),
        tupleBuffer.getOriginId());
}

HashMap* AggregationOperatorHandler::registerChunk(
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    std::unique_ptr<HashMap> finalAccumulator,
    std::vector<std::unique_ptr<HashMap>> inputs)
{
    /// Capture the raw pointer + input count BEFORE moving the owned maps into the registry.
    HashMap* const rawAccumulator = finalAccumulator.get();
    const auto numInputs = inputs.size();
    chunkRegistry.withWLock(
        [&](auto& registry)
        {
            /// Designated initializers pin field-by-name so a future reorder of ChunkMaps cannot silently swap the
            /// accumulator and the inputs vector (which would null the probe's finalHashMapPtr).
            const auto [it, inserted] = registry.emplace(
                std::pair{sequenceNumber, chunkNumber},
                ChunkMaps{.inputs = std::move(inputs), .finalAccumulator = std::move(finalAccumulator)});
            /// Keys are unique by construction: sequenceNumber is minted monotonically per triggered window and
            /// chunkNumber is unique per chunk within a window (INITIAL at P=1; INITIAL+p from 2d). A duplicate is a
            /// genuine logic error (a window triggered twice), so terminating via INVARIANT is correct.
            INVARIANT(inserted, "Duplicate chunk registration for sequenceNumber {} chunkNumber {}", sequenceNumber, chunkNumber);
        });
    chunksRegistered.fetch_add(1, std::memory_order_relaxed);
    NES_TRACE("Registered aggregation chunk seq={} chunk={} with {} input maps", sequenceNumber, chunkNumber, numInputs);
    return rawAccumulator;
}

void AggregationOperatorHandler::releaseChunk(const SequenceNumber sequenceNumber, const ChunkNumber chunkNumber)
{
    const auto erased
        = chunkRegistry.withWLock([&](auto& registry) { return registry.erase(std::pair{sequenceNumber, chunkNumber}); });
    if (erased > 0)
    {
        chunksReleased.fetch_add(1, std::memory_order_relaxed);
        NES_TRACE("Released aggregation chunk seq={} chunk={}", sequenceNumber, chunkNumber);
    }
}

void AggregationOperatorHandler::stop(
    const QueryTerminationType queryTerminationType, PipelineExecutionContext& pipelineExecutionContext)
{
    /// Teardown safety net: any chunk still in the registry was emitted but never drained (e.g. the buffer was dropped on
    /// engine teardown without the probe running). Freeing them here makes the design leak-total by construction.
    const auto swept = chunkRegistry.withWLock(
        [](auto& registry) -> std::size_t
        {
            const auto count = registry.size();
            registry.clear();
            return count;
        });
    if (swept > 0)
    {
        chunksReleased.fetch_add(swept, std::memory_order_relaxed);
        NES_DEBUG("Swept {} undrained aggregation chunk(s) from the registry at stop()", swept);
    }
    WindowBasedOperatorHandler::stop(queryTerminationType, pipelineExecutionContext);
}

void AggregationOperatorHandler::triggerAllWindows(PipelineExecutionContext* pipelineCtx)
{
    /// 2d seam. P is the configured grace-hash partition count of the (out-of-core) slice store, or 1 for an in-memory
    /// store. At P==1 we MUST be byte-identical to today, so we delegate VERBATIM to the base terminal flush (which
    /// ensure-residents + triggerSlices each window through the shared emitChunk helper with INITIAL/lastChunk=true/empty
    /// inputMaps). The dynamic_cast also fences the partitioned path to the spillable store that actually owns the
    /// claim/stream primitives.
    auto* const store = dynamic_cast<SpillableTimeBasedSliceStore*>(sliceAndWindowStore.get());
    const uint64_t numberOfPartitions = (store != nullptr) ? store->getNumberOfPartitions() : 1;
    if (numberOfPartitions == 1)
    {
        WindowBasedOperatorHandler::triggerAllWindows(pipelineCtx);
        return;
    }

    /// P>1 partitioned terminal flush. getAllNonTriggeredSlices() returns the retained band STILL spilled (lazy, L1); we
    /// process ONE window at a time so only 1/P of a single window is resident at any instant.
    auto windows = sliceAndWindowStore->getAllNonTriggeredSlices();
    NES_DEBUG(
        "triggerAllWindows: partitioned terminal flush of {} window(s) with P={} for origin {}",
        windows.size(),
        numberOfPartitions,
        outputOriginId);
    for (auto& [windowInfo, slices] : windows)
    {
        /// CLAIM the whole window once (mark every slice emitted so the LIVE spiller can never re-pick it). After this the
        /// slices' residency is stable, so the per-partition streaming reads below run safely without evictionMutex.
        store->claimWindowForPartitionedEmit(slices);

        /// Pass 1: materialise each partition and collect ONLY the non-empty ones (REVISED L3 skip-empty). Each entry is
        /// a self-contained chunk-to-be: its freshly-materialised per-worker maps, the accumulator built from the first
        /// of them, the raw pointers the probe combines, and the partition's tuple count.
        struct PendingChunk
        {
            std::vector<std::unique_ptr<HashMap>> maps;
            std::unique_ptr<HashMap> finalAccumulator;
            std::vector<HashMap*> ptrs;
            uint64_t tuples;
        };
        std::vector<PendingChunk> nonEmptyPartitions;
        for (uint64_t partition = 0; partition < numberOfPartitions; ++partition)
        {
            auto partitionMaps = store->streamWindowPartition(slices, partition);
            std::vector<HashMap*> ptrs;
            std::unique_ptr<HashMap> finalAccumulator;
            uint64_t partitionTuples = 0;
            for (const auto& map : partitionMaps)
            {
                if (map && map->getNumberOfTuples() > 0)
                {
                    ptrs.push_back(map.get());
                    partitionTuples += map->getNumberOfTuples();
                    if (not finalAccumulator)
                    {
                        finalAccumulator
                            = ChainedHashMap::createNewMapWithSameConfiguration(*dynamic_cast<ChainedHashMap*>(map.get()));
                    }
                }
            }
            /// SKIP-EMPTY: a partition with no entries is NOT emitted (an empty/null final map would crash the un-guardable
            /// JIT probe). It is simply dropped — its keys live in other partitions, so output is unchanged.
            if (partitionTuples > 0)
            {
                nonEmptyPartitions.push_back(
                    PendingChunk{
                        .maps = std::move(partitionMaps),
                        .finalAccumulator = std::move(finalAccumulator),
                        .ptrs = std::move(ptrs),
                        .tuples = partitionTuples});
            }
        }

        const auto numberOfNonEmptyPartitions = nonEmptyPartitions.size();
        if (numberOfNonEmptyPartitions == 0)
        {
            /// Degenerate fully-empty window (does not arise for a triggered window, but stay total). Re-materialise the
            /// window and emit ONE empty chunk exactly like the P=1 empty-window emit (null accumulator ⇒ registers nothing).
            NES_TRACE("triggerAllWindows: window {}-{} produced no non-empty partition; P=1-style empty emit",
                      windowInfo.windowInfo.windowStart, windowInfo.windowInfo.windowEnd);
            /// M2: these slices were already claimed by claimWindowForPartitionedEmit above. ensureWindowSlicesResident
            /// re-runs the SAME claim (claimSliceForEmit), which is IDEMPOTENT — emittedKeys.insert on an already-emitted
            /// sliceEnd is a no-op set insert, pinnedKeys.erase is harmless — then unspills any still-spilled slice. So
            /// the re-claim here is safe; it only guarantees residency for the (degenerate) empty-window single emit.
            sliceAndWindowStore->ensureWindowSlicesResident(slices);
            emitChunk(
                windowInfo.windowInfo,
                windowInfo.sequenceNumber,
                ChunkNumber(ChunkNumber::INITIAL),
                /*lastChunk=*/true,
                /*finalAccumulator=*/nullptr,
                /*inputMaps=*/{},
                /*allHashMapPtrs=*/{},
                /*numberOfTuples=*/0,
                pipelineCtx);
            continue;
        }

        /// Pass 2 — REVISED L3 contiguous-renumber: emit the K non-empty partitions as chunks [INITIAL, INITIAL+K-1],
        /// lastChunk on the K-th (highest-numbered). ChunkCollector completes the sequence on K contiguous chunks without
        /// P being known downstream, and every emitted chunk has >=1 entry (the probe's implicit P=1 invariant). The
        /// per-partition maps move into emitChunk → registerChunk, so the handler registry owns them (L8 leak-totality).
        for (uint64_t j = 0; j < numberOfNonEmptyPartitions; ++j)
        {
            auto& pending = nonEmptyPartitions[j];
            emitChunk(
                windowInfo.windowInfo,
                windowInfo.sequenceNumber,
                ChunkNumber(ChunkNumber::INITIAL + j),
                /*lastChunk=*/(j == numberOfNonEmptyPartitions - 1),
                std::move(pending.finalAccumulator),
                std::move(pending.maps),
                pending.ptrs,
                pending.tuples,
                pipelineCtx);
        }
    }
}

}
