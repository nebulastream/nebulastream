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


        /// We need a buffer that is large enough to store:
        /// - all pointers to all hashmaps of the window to be triggered
        /// - a new hashmap for the probe operator, so that we are not overwriting the thread local hashmaps
        /// - size of EmittedAggregationWindow
        const auto neededBufferSize = sizeof(EmittedAggregationWindow) + (allHashMaps.size() * sizeof(HashMap*));
        const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
        if (not tupleBufferVal.has_value())
        {
            throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededBufferSize);
        }
        auto tupleBuffer = tupleBufferVal.value();

        /// It might be that the buffer is not zeroed out.
        std::ranges::fill(tupleBuffer.getAvailableMemoryArea(), std::byte{0});

        /// As we are here "emitting" a buffer, we have to set the originId, the seq number, the watermark and the "number of tuples".
        /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setSequenceNumber(windowInfo.sequenceNumber);
        tupleBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        tupleBuffer.setLastChunk(true);
        tupleBuffer.setWatermark(windowInfo.windowInfo.windowStart);
        tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
        tupleBuffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));


        /// 2c0: the final accumulator is owned by the handler's chunkRegistry (NOT the recycle-only emit buffer), keyed by
        /// (sequenceNumber, chunkNumber); it is freed when the probe finishes draining this chunk (releaseChunk) or swept
        /// by stop() at teardown. The emitted buffer carries only the raw, non-owning accumulator pointer, which keeps the
        /// buffer trivially destructible so a dropped-undrained buffer cannot leak. A window with no non-empty slices has a
        /// null accumulator and registers nothing (byte-identical to the previous null unique_ptr being moved in).
        HashMap* finalHashMapPtr = nullptr;
        if (finalHashMap)
        {
            finalHashMapPtr = registerChunk(windowInfo.sequenceNumber, ChunkNumber(ChunkNumber::INITIAL), std::move(finalHashMap));
        }

        /// Writing all necessary information for the aggregation probe to the buffer via the placement new constructor
        auto tmp = tupleBuffer.getAvailableMemoryArea();
        new (tmp.data()) EmittedAggregationWindow{
            windowInfo.windowInfo, finalHashMapPtr, windowInfo.sequenceNumber, ChunkNumber(ChunkNumber::INITIAL), allHashMaps};


        /// Dispatching the buffer to the probe operator via the task queue.
        pipelineCtx->emitBuffer(tupleBuffer);
        NES_TRACE(
            "Emitted window {}-{} with watermarkTs {} sequenceNumber {} originId {}",
            windowInfo.windowInfo.windowStart,
            windowInfo.windowInfo.windowEnd,
            tupleBuffer.getWatermark(),
            tupleBuffer.getSequenceNumber(),
            tupleBuffer.getOriginId());
    }
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

}
