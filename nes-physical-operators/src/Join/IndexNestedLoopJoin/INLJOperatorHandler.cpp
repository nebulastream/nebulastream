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

#include <Join/IndexNestedLoopJoin/INLJOperatorHandler.hpp>

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Join/IndexNestedLoopJoin/INLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

INLJOperatorHandler::INLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    JoinTriggerStrategy triggerStrategy)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), std::move(triggerStrategy))
{
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
INLJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& args) const
{
    PRECONDITION(numberOfWorkerThreads > 0, "Number of worker threads must be set before creating INLJ slices");
    const auto& inljArgs = dynamic_cast<const CreateNewINLJSliceArgs&>(args);
    return std::function(
        [numberOfWorkerThreads = numberOfWorkerThreads,
         bufferProvider = inljArgs.bufferProvider,
         tupleSizeLeft = inljArgs.tupleSizeLeft,
         tupleSizeRight = inljArgs.tupleSizeRight](const SliceStart start, const SliceEnd end)
        {
            return std::vector<std::shared_ptr<Slice>>{std::make_shared<INLJSlice>(
                *bufferProvider, start, end, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight)};
        });
}

void INLJOperatorHandler::emitSlicesToProbe(
    const std::vector<std::shared_ptr<Slice>>& leftSlices,
    const std::vector<std::shared_ptr<Slice>>& rightSlices,
    const ProbeTaskType probeTaskType,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    if (probeTaskType != ProbeTaskType::MATCH_PAIRS)
    {
        throw NotImplemented("The INLJ prototype supports inner joins only");
    }
    uint64_t totalNumberOfTuples = 0;
    std::vector<SliceEnd> leftSliceEnds;
    std::vector<SliceEnd> rightSliceEnds;
    leftSliceEnds.reserve(leftSlices.size());
    rightSliceEnds.reserve(rightSlices.size());
    for (const auto& slice : leftSlices)
    {
        const auto& inljSlice = dynamic_cast<const INLJSlice&>(*slice);
        totalNumberOfTuples += inljSlice.getNumberOfTuples(JoinBuildSideType::Left);
        leftSliceEnds.emplace_back(slice->getSliceEnd());
    }
    for (const auto& slice : rightSlices)
    {
        const auto& inljSlice = dynamic_cast<const INLJSlice&>(*slice);
        totalNumberOfTuples += inljSlice.getNumberOfTuples(JoinBuildSideType::Right);
        rightSliceEnds.emplace_back(slice->getSliceEnd());
    }

    const auto neededBufferSize
        = sizeof(EmittedINLJWindowTrigger) + ((leftSliceEnds.size() + rightSliceEnds.size()) * sizeof(SliceEnd));
    auto buffer = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not buffer.has_value())
    {
        throw CannotAllocateBuffer("{}B were requested for the INLJ window trigger", neededBufferSize);
    }
    buffer->setOriginId(outputOriginId);
    buffer->setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    buffer->setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    buffer->setLastChunk(sequenceData.lastChunk);
    buffer->setWatermark(windowInfo.windowStart);
    buffer->setNumberOfTuples(totalNumberOfTuples);
    buffer->setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    new (buffer->getAvailableMemoryArea().data()) EmittedINLJWindowTrigger{windowInfo, leftSliceEnds, rightSliceEnds};
    pipelineCtx->emitBuffer(*buffer);
}

}
