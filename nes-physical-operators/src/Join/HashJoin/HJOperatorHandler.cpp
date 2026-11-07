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


#include <Join/HashJoin/HJOperatorHandler.hpp>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// Collects non-empty hash map buffers for one build side from a single slice. Read-only: a hash map that no
/// build worker ever touched for this slice is simply skipped, exactly as an untouched slot would be, rather
/// than being lazily created here. Trigger-time must never allocate, since that would race with a build worker
/// concurrently first-touching the same slot (see HashMapSlice::getOrCreateHashMapBufferRef).
std::vector<TupleBuffer> getHashMapsFromSlice(const Slice& slice, JoinBuildSideType side)
{
    std::vector<TupleBuffer> buffers;
    const auto* hjSlice = dynamic_cast<const HJSlice*>(&slice);
    INVARIANT(hjSlice != nullptr, "Slice must be of type HJSlice!");
    for (uint64_t i = 0; i < hjSlice->getNumberOfHashMapsForSide(); ++i)
    {
        if (const auto* hashMapBuffer = hjSlice->getHashMapBufferRefForSide(WorkerThreadId(i), side);
            hashMapBuffer != nullptr && ChainedHashMap::load(*hashMapBuffer).getTotalNumberOfRecords() > 0)
        {
            buffers.emplace_back(*hashMapBuffer);
        }
    }
    return buffers;
}

/// Collects non-empty hash map buffers from multiple slices for one build side
std::vector<TupleBuffer> getHashMapsFromSlices(const std::vector<std::shared_ptr<Slice>>& slices, JoinBuildSideType side)
{
    std::vector<TupleBuffer> allBuffers;
    for (const auto& slice : slices)
    {
        for (auto& buffer : getHashMapsFromSlice(*slice, side))
        {
            allBuffers.emplace_back(buffer);
        }
    }
    return allBuffers;
}
}

HJOperatorHandler::HJOperatorHandler(
    SourcesOfInputOrigins sourcesOfInputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<SlicedWindowStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy,
    const bool unboundedWindow)
    : StreamJoinOperatorHandler(
          std::move(sourcesOfInputOrigins),
          outputOriginId,
          std::move(sliceAndWindowStore),
          std::move(triggerStrategy),
          /// Compaction merges a segment's per-thread and per-slice maps into one map per side, so that a probe looks up a key in
          /// O(log #slices) maps instead of in one map per slice and worker thread.
          unboundedWindow ? std::make_unique<UnboundedJoinSegments>(true) : nullptr)
    , setupAlreadyCalledLeft(false)
    , setupAlreadyCalledRight(false)
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
HJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Has setWorkerThreads() being called?");

    const auto& newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    return std::function(
        [outputOriginId = outputOriginId, numberOfWorkerThreads = numberOfWorkerThreads, copyOfNewHashMapArgs = newHashMapArgs](
            SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new hash-join slice for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<HJSlice>(sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads)};
        });
}

bool HJOperatorHandler::wasSetupCalled(const JoinBuildSideType& buildSide)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right: {
            bool expectedValue = false;
            return not setupAlreadyCalledRight.compare_exchange_strong(expectedValue, true);
        }
        case JoinBuildSideType::Left: {
            bool expectedValue = false;
            return not setupAlreadyCalledLeft.compare_exchange_strong(expectedValue, true);
        }
    }

    std::unreachable();
}

void HJOperatorHandler::emitSlicesToProbe(
    const std::vector<std::shared_ptr<Slice>>& leftSlices,
    const std::vector<std::shared_ptr<Slice>>& rightSlices,
    ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    emitJoinStateToProbe(
        getHashMapsFromSlices(leftSlices, JoinBuildSideType::Left),
        getHashMapsFromSlices(rightSlices, JoinBuildSideType::Right),
        probeTaskType,
        windowInfo,
        sequenceData,
        0,
        pipelineCtx);
}

std::vector<TupleBuffer>
HJOperatorHandler::getJoinStateOfSlices(const std::vector<std::shared_ptr<Slice>>& slices, const JoinBuildSideType side)
{
    return getHashMapsFromSlices(slices, side);
}

uint64_t HJOperatorHandler::getNumberOfTuples(const TupleBuffer& joinState) const
{
    return ChainedHashMap::load(joinState).getTotalNumberOfRecords();
}

}
