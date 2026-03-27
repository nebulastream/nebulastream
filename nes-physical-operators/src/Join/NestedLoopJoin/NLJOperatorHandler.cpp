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
    PRECONDITION(probeTaskType == ProbeTaskType::MATCH_PAIRS, "NLJ currently only supports MATCH_PAIRS probe tasks");
    PRECONDITION(leftSlices.size() == 1 && rightSlices.size() == 1, "MATCH_PAIRS expects exactly one slice per side");

    auto& nljSliceLeft = dynamic_cast<NLJSlice&>(*leftSlices[0]);
    auto& nljSliceRight = dynamic_cast<NLJSlice&>(*rightSlices[0]);

    nljSliceLeft.combinePagedVectors();
    nljSliceRight.combinePagedVectors();
    const auto totalNumberOfTuples = nljSliceLeft.getNumberOfTuplesLeft() + nljSliceRight.getNumberOfTuplesRight();

    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();

    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    new (tupleBuffer.getAvailableMemoryArea().data())
        EmittedNLJWindowTrigger{windowInfo, leftSlices[0]->getSliceEnd(), rightSlices[0]->getSliceEnd()};

    pipelineCtx->emitBuffer(tupleBuffer);

    NES_TRACE(
        "Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left tuples "
        "{} and no. right tuples {} for window info: {}-{}",
        leftSlices[0]->getSliceEnd(),
        rightSlices[0]->getSliceEnd(),
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceDataAsString(),
        tupleBuffer.getOriginId(),
        nljSliceLeft.getNumberOfTuplesLeft(),
        nljSliceRight.getNumberOfTuplesRight(),
        windowInfo.windowStart,
        windowInfo.windowEnd);
}

}
