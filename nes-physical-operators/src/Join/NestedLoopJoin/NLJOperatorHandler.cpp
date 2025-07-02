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
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
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
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
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
    Slice& sliceLeft,
    Slice& sliceRight,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    auto& nljSliceLeft = dynamic_cast<NLJSlice&>(sliceLeft);
    auto& nljSliceRight = dynamic_cast<NLJSlice&>(sliceRight);

    nljSliceLeft.combinePagedVectors();
    nljSliceRight.combinePagedVectors();
    const auto totalNumberOfTuples = nljSliceLeft.getNumberOfTuplesLeft() + nljSliceRight.getNumberOfTuplesRight();

    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();

    /// As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
    /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);

    auto* bufferMemory = tupleBuffer.getBuffer<EmittedNLJWindowTrigger>();
    bufferMemory->leftSliceEnd = sliceLeft.getSliceEnd();
    bufferMemory->rightSliceEnd = sliceRight.getSliceEnd();
    bufferMemory->windowInfo = windowInfo;

    /// Dispatching the buffer to the probe operator via the task queue.
    pipelineCtx->emitBuffer(tupleBuffer);

    NES_DEBUG(
        "Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left tuples "
        "{} and no. right tuples {} for window info: {}-{}",
        bufferMemory->leftSliceEnd,
        bufferMemory->rightSliceEnd,
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceDataAsString(),
        tupleBuffer.getOriginId(),
        nljSliceLeft.getNumberOfTuplesLeft(),
        nljSliceRight.getNumberOfTuplesRight(),
        windowInfo.windowStart,
        windowInfo.windowEnd);
}

}
