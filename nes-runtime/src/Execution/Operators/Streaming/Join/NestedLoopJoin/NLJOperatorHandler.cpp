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
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <optional>

namespace NES::Runtime::Execution::Operators {

NLJOperatorHandler::NLJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                       const OriginId outputOriginId,
                                       uint64_t sizeOfTupleInByteLeft,
                                       uint64_t sizeOfTupleInByteRight,
                                       uint64_t sizePageLeft,
                                       uint64_t sizePageRight,
                                       uint64_t windowSize,
                                       uint64_t windowSlide)
    : StreamJoinOperatorHandler(inputOrigins,
                                outputOriginId,
                                windowSize,
                                windowSlide,
                                QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
                                sizeOfTupleInByteLeft,
                                sizeOfTupleInByteRight),
      leftPageSize(sizePageLeft), rightPageSize(sizePageRight) {}

void NLJOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) { NES_DEBUG("start NLJOperatorHandler"); }

uint64_t NLJOperatorHandler::getNumberOfTuplesInWindow(uint64_t sliceIdentifier,
                                                       QueryCompilation::JoinBuildSideType joinBuildSide) {
    const auto slice = getSliceBySliceIdentifier(sliceIdentifier);
    if (slice.has_value()) {
        auto& sliceVal = slice.value();
        switch (joinBuildSide) {
            case QueryCompilation::JoinBuildSideType::Left: return sliceVal->getNumberOfTuplesLeft();
            case QueryCompilation::JoinBuildSideType::Right: return sliceVal->getNumberOfTuplesRight();
        }
    }

    return -1;
}

void NLJOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) { NES_DEBUG("stop NLJOperatorHandler"); }

void NLJOperatorHandler::triggerSlices(TriggerableWindows& triggerableSlices,
                                        PipelineExecutionContext* pipelineCtx) {
    /**
     * We expect the sliceIdentifiersToBeTriggered to be sorted.
     * Otherwise, it can happen that the buffer <seq 2, ts 1000> gets processed after <seq 1, ts 20000>, which will lead to wrong results upstream
     * Also, we have to set the seq number in order of the slice end timestamps.
     * Furthermore, we expect the sliceIdentifier to be the sliceEnd.
     */
    for (const auto& [windowId, slices] : triggerableSlices.windowIdToTriggerableSlices) {
        for (auto& [curSliceLeft, curSliceRight] : getSlicesLeftRightForWindow(windowId)) {
            if (curSliceLeft->getNumberOfTuplesLeft() > 0 && curSliceRight->getNumberOfTuplesRight() > 0) {

                std::dynamic_pointer_cast<NLJSlice>(curSliceLeft)->combinePagedVectors();
                std::dynamic_pointer_cast<NLJSlice>(curSliceRight)->combinePagedVectors();

                auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
                auto bufferMemory = tupleBuffer.getBuffer<EmittedNLJWindowTriggerTask>();
                bufferMemory->leftSliceIdentifier = curSliceLeft->getSliceIdentifier();
                bufferMemory->rightSliceIdentifier = curSliceRight->getSliceIdentifier();
                bufferMemory->windowInfo = slices.windowInfo;
                tupleBuffer.setNumberOfTuples(1);

                // As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
                // As we have a global watermark that is larger than the slice end, we can set the watermarkTs to be the slice end.
                tupleBuffer.setOriginId(getOutputOriginId());
                tupleBuffer.setSequenceNumber(getNextSequenceNumber());
                tupleBuffer.setWatermark(std::min(curSliceLeft->getSliceEnd(), curSliceRight->getSliceEnd()));

                pipelineCtx->dispatchBuffer(tupleBuffer);
                NES_INFO("Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left tuples {} and no. right tuples {}",
                         bufferMemory->leftSliceIdentifier,
                         bufferMemory->rightSliceIdentifier,
                         tupleBuffer.getWatermark(),
                         tupleBuffer.getSequenceNumber(),
                         tupleBuffer.getOriginId(),
                         curSliceLeft->getNumberOfTuplesLeft(),
                         curSliceRight->getNumberOfTuplesRight());
            }
        }
    }
}

NLJOperatorHandlerPtr NLJOperatorHandler::create(const std::vector<OriginId>& inputOrigins,
                                                 const OriginId outputOriginId,
                                                 const uint64_t sizeOfTupleInByteLeft,
                                                 const uint64_t sizeOfTupleInByteRight,
                                                 const uint64_t sizePageLeft,
                                                 const uint64_t rightPageSize,
                                                 const uint64_t windowSize,
                                                 const uint64_t windowSlide) {

    return std::make_shared<NLJOperatorHandler>(inputOrigins,
                                                outputOriginId,
                                                sizeOfTupleInByteLeft,
                                                sizeOfTupleInByteRight,
                                                sizePageLeft,
                                                rightPageSize,
                                                windowSize,
                                                windowSlide);
}

StreamSlice* NLJOperatorHandler::getCurrentWindowOrCreate() {
    if (slices.rlock()->empty()) {
        return getSliceByTimestampOrCreateIt(0).get();
    }
    return slices.rlock()->back().get();
}

uint64_t NLJOperatorHandler::getLeftPageSize() const { return leftPageSize; }

uint64_t NLJOperatorHandler::getRightPageSize() const { return rightPageSize; }

void* getNLJPagedVectorProxy(void* ptrNljSlice, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNljSlice != nullptr, "nlj slice pointer should not be null!");
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto* nljSlice = static_cast<NLJSlice*>(ptrNljSlice);
    switch (joinBuildSide) {
        case QueryCompilation::JoinBuildSideType::Left: return nljSlice->getPagedVectorRefLeft(workerId);
        case QueryCompilation::JoinBuildSideType::Right: return nljSlice->getPagedVectorRefRight(workerId);
    }
}
}// namespace NES::Runtime::Execution::Operators