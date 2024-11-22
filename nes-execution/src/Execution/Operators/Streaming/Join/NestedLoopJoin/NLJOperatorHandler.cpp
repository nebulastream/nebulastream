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
    averageNumberOfTuplesLeft.wlock()->first = static_cast<int64_t>(leftMemoryProvider->getMemoryLayoutPtr()->getCapacity());
    averageNumberOfTuplesLeft.wlock()->second = 0;
    averageNumberOfTuplesRight.wlock()->first = static_cast<int64_t>(rightMemoryProvider->getMemoryLayoutPtr()->getCapacity());
    averageNumberOfTuplesRight.wlock()->second = 0;
}

std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> NLJOperatorHandler::getCreateNewSlicesFunction() const
{
    return std::function(
        [&](SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            const auto [averageNumberOfTuplesLeft, _] = *this->averageNumberOfTuplesLeft.rlock();
            const auto [averageNumberOfTuplesRight, __] = *this->averageNumberOfTuplesLeft.rlock();
            auto memoryLayoutCopyLeft = leftMemoryProvider->getMemoryLayoutPtr()->deepCopy();
            auto memoryLayoutCopyRight = rightMemoryProvider->getMemoryLayoutPtr()->deepCopy();

            auto newBufferSizeLeft = averageNumberOfTuplesLeft * memoryLayoutCopyLeft->getTupleSize();
            auto newBufferSizeRight = averageNumberOfTuplesRight * memoryLayoutCopyRight->getTupleSize();
            newBufferSizeLeft = std::max(bufferProvider->getBufferSize(), newBufferSizeLeft);
            newBufferSizeRight = std::max(bufferProvider->getBufferSize(), newBufferSizeRight);

            memoryLayoutCopyLeft->setBufferSize(newBufferSizeLeft);
            memoryLayoutCopyRight->setBufferSize(newBufferSizeRight);


            NES_INFO("Creating new NLJ slice for sliceStart {} and sliceEnd {}", sliceStart, sliceEnd);
            return {std::make_shared<NLJSlice>(
                sliceStart, sliceEnd, numberOfWorkerThreads, bufferProvider, memoryLayoutCopyLeft, memoryLayoutCopyRight)};
        });
}

void NLJOperatorHandler::emitSliceIdsToProbe(
    Slice& sliceLeft,
    Slice& sliceRight,
    const WindowInfoAndSequenceNumber& windowInfoAndSeqNumber,
    const ChunkNumber& chunkNumber,
    const bool isLastChunk,
    PipelineExecutionContext* pipelineCtx)
{
    dynamic_cast<NLJSlice&>(sliceLeft).combinePagedVectors();
    dynamic_cast<NLJSlice&>(sliceRight).combinePagedVectors();

    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    tupleBuffer.setNumberOfTuples(1);

    /// As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
    /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(windowInfoAndSeqNumber.sequenceNumber);
    tupleBuffer.setChunkNumber(chunkNumber);
    tupleBuffer.setLastChunk(isLastChunk);
    tupleBuffer.setWatermark(Timestamp(std::min(sliceLeft.getSliceStart().getRawValue(), sliceRight.getSliceStart().getRawValue())));

    auto* bufferMemory = tupleBuffer.getBuffer<EmittedNLJWindowTriggerTask>();
    bufferMemory->leftSliceEnd = sliceLeft.getSliceEnd();
    bufferMemory->rightSliceEnd = sliceRight.getSliceEnd();
    bufferMemory->windowInfo = windowInfoAndSeqNumber.windowInfo;

    pipelineCtx->emitBuffer(tupleBuffer, PipelineExecutionContext::ContinuationPolicy::NEVER);
    NES_DEBUG(
        "Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left tuples "
        "{} and no. right tuples {} for window info: {}-{}",
        bufferMemory->leftSliceEnd,
        bufferMemory->rightSliceEnd,
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceNumber(),
        tupleBuffer.getOriginId(),
        sliceLeft.getNumberOfTuplesLeft(),
        sliceRight.getNumberOfTuplesRight(),
        windowInfoAndSeqNumber.windowInfo.windowStart,
        windowInfoAndSeqNumber.windowInfo.windowEnd);

    /// Calculating a rolling average of the number of tuples in the slices
    const auto averageNumberLockedLeft = this->averageNumberOfTuplesLeft.wlock();
    averageNumberLockedLeft->second = std::min(averageNumberLockedLeft->second + 1, windowSizeRollingAverage);
    averageNumberLockedLeft->first
        += (static_cast<int64_t>(sliceLeft.getNumberOfTuplesLeft()) - averageNumberLockedLeft->first) / averageNumberLockedLeft->second;

    const auto averageNumberLockedRight = this->averageNumberOfTuplesRight.wlock();
    averageNumberLockedRight->second = std::min(averageNumberLockedRight->second + 1, windowSizeRollingAverage);
    averageNumberLockedRight->first
        += (static_cast<int64_t>(sliceRight.getNumberOfTuplesRight()) - averageNumberLockedRight->first) / averageNumberLockedRight->second;
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
