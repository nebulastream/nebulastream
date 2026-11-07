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
#include <Interface/BTree/BTree.hpp>
#include <Join/IndexNestedLoopJoin/INLJSlice.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

INLJOperatorHandler::INLJOperatorHandler(
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
          /// Compaction merges a segment's B-trees into one B-tree per side. Without it, a probe would search one B-tree per slice
          /// and worker thread, so its cost would grow linearly with the number of slices instead of logarithmically.
          unboundedWindow ? std::make_unique<UnboundedJoinSegments>(true) : nullptr)
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
            return std::vector<std::shared_ptr<Slice>>{
                std::make_shared<INLJSlice>(*bufferProvider, start, end, numberOfWorkerThreads, tupleSizeLeft, tupleSizeRight)};
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

    const auto neededBufferSize = sizeof(EmittedINLJWindowTrigger) + ((leftSliceEnds.size() + rightSliceEnds.size()) * sizeof(SliceEnd));
    std::optional<TupleBuffer> buffer
        = getBufferOfAtLeast(neededBufferSize, *pipelineCtx->getBufferManager(), "The trigger of an index nested-loop join probe");
    buffer->setOriginId(outputOriginId);
    buffer->setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    buffer->setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    buffer->setLastChunk(sequenceData.lastChunk);
    buffer->setWatermark(windowInfo.windowStart);
    buffer->setNumberOfTuples(totalNumberOfTuples);
    buffer->setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
    new (buffer->getAvailableMemoryArea().data()) EmittedINLJWindowTrigger{windowInfo, leftSliceEnds, rightSliceEnds, probeTaskType};
    pipelineCtx->emitBuffer(*buffer);
}

std::vector<TupleBuffer>
INLJOperatorHandler::getJoinStateOfSlices(const std::vector<std::shared_ptr<Slice>>& slices, const JoinBuildSideType side)
{
    std::vector<TupleBuffer> trees;
    for (const auto& slice : slices)
    {
        const auto& inljSlice = dynamic_cast<const INLJSlice&>(*slice);
        for (uint64_t index = 0; index < inljSlice.getNumberOfBTrees(side); ++index)
        {
            if (const auto* tree = inljSlice.getBTreeBuffer(index, side); BTree::load(*tree).size() > 0)
            {
                trees.emplace_back(*tree);
            }
        }
    }
    return trees;
}

uint64_t INLJOperatorHandler::getNumberOfTuples(const TupleBuffer& joinState) const
{
    return BTree::load(joinState).size();
}

}
