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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators
{
NLJOperatorHandler::NLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), leftMemoryProvider, rightMemoryProvider)
{
    averageNumberOfTuplesLeft.wlock()->first = static_cast<int64_t>(leftMemoryProvider->getMemoryLayout()->getCapacity());
    averageNumberOfTuplesLeft.wlock()->second = 0;
    averageNumberOfTuplesRight.wlock()->first = static_cast<int64_t>(rightMemoryProvider->getMemoryLayout()->getCapacity());
    averageNumberOfTuplesRight.wlock()->second = 0;
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> NLJOperatorHandler::getCreateNewSlicesFunction() const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");
    return std::function(
        [this](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_INFO("Creating new NLJ slice for sliceStart {} and sliceEnd {}", sliceStart, sliceEnd);
            return {std::make_shared<NLJSlice>(sliceStart, sliceEnd, numberOfWorkerThreads)};
        });
}

void NLJOperatorHandler::emitSliceIdsToProbe(
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

    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    tupleBuffer.setNumberOfTuples(1);

    /// As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
    /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);

    auto* bufferMemory = tupleBuffer.getBuffer<EmittedNLJWindowTrigger>();
    bufferMemory->leftSliceEnd = sliceLeft.getSliceEnd();
    bufferMemory->rightSliceEnd = sliceRight.getSliceEnd();
    bufferMemory->windowInfo = windowInfo;

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

    /// Calculating a rolling average of the number of tuples in the slices
    const auto averageNumberLeft = this->averageNumberOfTuplesLeft.wlock();
    averageNumberLeft->second = std::min(averageNumberLeft->second + 1, windowSizeRollingAverage);
    averageNumberLeft->first
        += (static_cast<int64_t>(nljSliceLeft.getNumberOfTuplesLeft()) - averageNumberLeft->first) / averageNumberLeft->second;

    const auto averageNumberRight = this->averageNumberOfTuplesRight.wlock();
    averageNumberRight->second = std::min(averageNumberRight->second + 1, windowSizeRollingAverage);
    averageNumberRight->first
        += (static_cast<int64_t>(nljSliceRight.getNumberOfTuplesRight()) - averageNumberRight->first) / averageNumberRight->second;
}

Nautilus::Interface::PagedVector* getNLJPagedVectorProxy(
    const NLJSlice* nljSlice, const WorkerThreadId workerThreadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left:
            return nljSlice->getPagedVectorRefLeft(workerThreadId);
        case QueryCompilation::JoinBuildSideType::Right:
            return nljSlice->getPagedVectorRefRight(workerThreadId);
    }
}
}
