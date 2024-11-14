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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::Runtime::Execution::Operators
{

NLJOperatorHandler::NLJOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    const uint64_t windowSize,
    const uint64_t windowSlide,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, leftMemoryProvider, rightMemoryProvider)
{
}

std::shared_ptr<StreamJoinSlice> NLJOperatorHandler::createNewSlice(const SliceStart sliceStart, const SliceEnd sliceEnd)
{
    return std::make_shared<NLJSlice>(
        sliceStart,
        sliceEnd,
        numberOfWorkerThreads,
        bufferProvider,
        leftMemoryProvider->getMemoryLayoutPtr(),
        rightMemoryProvider->getMemoryLayoutPtr());
}

void NLJOperatorHandler::emitSliceIdsToProbe(
    StreamJoinSlice& sliceLeft, StreamJoinSlice& sliceRight, const WindowInfo& windowInfo, PipelineExecutionContext* pipelineCtx)
{
    if (sliceLeft.getNumberOfTuplesLeft() > 0 && sliceRight.getNumberOfTuplesRight() > 0)
    {
        dynamic_cast<NLJSlice&>(sliceLeft).combinePagedVectors();
        dynamic_cast<NLJSlice&>(sliceRight).combinePagedVectors();

        auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
        tupleBuffer.setNumberOfTuples(1);

        /// As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
        /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
        tupleBuffer.setOriginId(outputOriginId);
        tupleBuffer.setSequenceNumber(SequenceNumber(getNextSequenceNumber()));
        tupleBuffer.setWatermark(Timestamp(std::min(sliceLeft.getSliceStart().getRawValue(), sliceRight.getSliceStart().getRawValue())));

        auto* bufferMemory = tupleBuffer.getBuffer<EmittedNLJWindowTriggerTask>();
        bufferMemory->leftSliceEnd = sliceLeft.getSliceEnd();
        bufferMemory->rightSliceEnd = sliceRight.getSliceEnd();
        bufferMemory->windowInfo = windowInfo;

        pipelineCtx->dispatchBuffer(tupleBuffer);
        NES_INFO(
            "Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left tuples "
            "{} and no. right tuples {} for window info: {}-{}",
            bufferMemory->leftSliceEnd,
            bufferMemory->rightSliceEnd,
            tupleBuffer.getWatermark(),
            tupleBuffer.getSequenceNumber(),
            tupleBuffer.getOriginId(),
            sliceLeft.getNumberOfTuplesLeft(),
            sliceRight.getNumberOfTuplesRight(),
            windowInfo.windowStart,
            windowInfo.windowEnd);
    }
}

Nautilus::Interface::PagedVector* getNLJPagedVectorProxy(
    const NLJSlice* nljSlice, const WorkerThreadId::Underlying workerThreadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
{
    PRECONDITION(nljSlice != nullptr, "nlj slice pointer should not be null!");
    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left:
            return nljSlice->getPagedVectorRefLeft(WorkerThreadId(workerThreadId));
        case QueryCompilation::JoinBuildSideType::Right:
            return nljSlice->getPagedVectorRefRight(WorkerThreadId(workerThreadId));
    }
}
}
