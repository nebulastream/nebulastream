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

#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
NLJOperatorHandler::NLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
NLJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments&) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    return std::function(
        [numberOfWorkerThreads = numberOfWorkerThreads,
         outputOriginId = outputOriginId](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE(
                "Creating new NLJ slice for sliceStart {} and sliceEnd {} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<NLJSlice>(sliceStart, sliceEnd, numberOfWorkerThreads)};
        });
}

void NLJOperatorHandler::emitSlicesToProbe(
    const std::vector<std::shared_ptr<Slice>>& leftSlices,
    const std::vector<std::shared_ptr<Slice>>& rightSlices,
    ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    /// Combine paged vectors for all slices on both sides
    uint64_t totalNumberOfTuples = 0;
    for (const auto& slice : leftSlices)
    {
        auto& nljSlice = dynamic_cast<NLJSlice&>(*slice);
        nljSlice.combinePagedVectors();
        totalNumberOfTuples += nljSlice.getNumberOfTuplesLeft();
    }
    for (const auto& slice : rightSlices)
    {
        auto& nljSlice = dynamic_cast<NLJSlice&>(*slice);
        nljSlice.combinePagedVectors();
        totalNumberOfTuples += nljSlice.getNumberOfTuplesRight();
    }

    /// Collect slice ends
    std::vector<SliceEnd> leftSliceEnds;
    leftSliceEnds.reserve(leftSlices.size());
    for (const auto& slice : leftSlices)
    {
        leftSliceEnds.emplace_back(slice->getSliceEnd());
    }
    std::vector<SliceEnd> rightSliceEnds;
    rightSliceEnds.reserve(rightSlices.size());
    for (const auto& slice : rightSlices)
    {
        rightSliceEnds.emplace_back(slice->getSliceEnd());
    }

    /// Allocate variable-sized trigger buffer
    const auto neededBufferSize = sizeof(EmittedNLJWindowTrigger) + ((leftSliceEnds.size() + rightSliceEnds.size()) * sizeof(SliceEnd));
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the NLJ window trigger were requested", neededBufferSize);
    }

    auto tupleBuffer = tupleBufferVal.value();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    new (tupleBuffer.getAvailableMemoryArea().data()) EmittedNLJWindowTrigger{windowInfo, leftSliceEnds, rightSliceEnds, probeTaskType};

    pipelineCtx->emitBuffer(tupleBuffer);
}

}
