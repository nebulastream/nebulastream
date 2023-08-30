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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {
void NLJOperatorHandler::emitSliceIdsToProbe(StreamSlice& sliceLeft, StreamSlice& sliceRight, const WindowInfo& windowInfo,
                                             PipelineExecutionContext* pipelineCtx) {
    if (sliceLeft.getNumberOfTuplesLeft() > 0 && sliceRight.getNumberOfTuplesRight() > 0) {
        dynamic_cast<NLJSlice&>(sliceLeft).combinePagedVectors();
        dynamic_cast<NLJSlice&>(sliceRight).combinePagedVectors();

        auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
        auto bufferMemory = tupleBuffer.getBuffer<EmittedNLJWindowTriggerTask>();
        bufferMemory->leftSliceIdentifier = sliceLeft.getSliceIdentifier();
        bufferMemory->rightSliceIdentifier = sliceRight.getSliceIdentifier();
        bufferMemory->windowInfo = windowInfo;
        tupleBuffer.setNumberOfTuples(1);

        // As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
        // As we have a global watermark that is larger than the slice end, we can set the watermarkTs to be the slice end.
        tupleBuffer.setOriginId(getOutputOriginId());
        tupleBuffer.setSequenceNumber(getNextSequenceNumber());
        tupleBuffer.setWatermark(std::min(sliceLeft.getSliceEnd(), sliceRight.getSliceEnd()));

        pipelineCtx->dispatchBuffer(tupleBuffer);
        NES_INFO("Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left tuples {} and no. right tuples {}",
                 bufferMemory->leftSliceIdentifier,
                 bufferMemory->rightSliceIdentifier,
                 tupleBuffer.getWatermark(),
                 tupleBuffer.getSequenceNumber(),
                 tupleBuffer.getOriginId(),
                 sliceLeft.getNumberOfTuplesLeft(),
                 sliceRight.getNumberOfTuplesRight());
    }
}

StreamSlicePtr NLJOperatorHandler::createNewSlice(uint64_t sliceStart, uint64_t sliceEnd) {
    return std::make_shared<NLJSlice>(sliceStart,
                                      sliceEnd,
                                      numberOfWorkerThreads,
                                      sizeOfRecordLeft,
                                      pageSizeLeft,
                                      sizeOfRecordRight,
                                      pageSizeRight);
}

NLJOperatorHandler::NLJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                       const OriginId outputOriginId,
                                       const uint64_t windowSize,
                                       const uint64_t windowSlide,
                                       const uint64_t sizeOfRecordLeft,
                                       const uint64_t sizeOfRecordRight,
                                       const uint64_t pageSizeLeft,
                                       const uint64_t pageSizeRight)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide,sizeOfRecordLeft, sizeOfRecordRight),
      pageSizeLeft(pageSizeLeft), pageSizeRight(pageSizeRight) {}

void* getNLJPagedVectorProxy(void* ptrNljSlice, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    NES_DEBUG("nljSlice:\n{}", nljSlice->toString());
    switch (joinBuildSide) {
        case QueryCompilation::JoinBuildSideType::Left: return nljSlice->getPagedVectorRefLeft(workerId);
        case QueryCompilation::JoinBuildSideType::Right: return nljSlice->getPagedVectorRefRight(workerId);
    }
}


}; // namespace NES::Runtime::Execution::Operators