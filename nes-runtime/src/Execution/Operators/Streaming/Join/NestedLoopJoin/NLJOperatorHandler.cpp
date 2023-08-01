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
#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
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
                                       uint64_t windowSize)
    : StreamJoinOperatorHandler(inputOrigins,
                                outputOriginId,
                                windowSize,
                                QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
                                sizeOfTupleInByteLeft,
                                sizeOfTupleInByteRight),
      leftPageSize(sizePageLeft), rightPageSize(sizePageRight) {}

void NLJOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) { NES_DEBUG("start NLJOperatorHandler"); }

uint64_t NLJOperatorHandler::getNumberOfTuplesInWindow(uint64_t windowIdentifier,
                                                       QueryCompilation::JoinBuildSideType joinBuildSide) {
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        auto& windowVal = window.value();
        switch (joinBuildSide) {
            case QueryCompilation::JoinBuildSideType::Left: return windowVal->getNumberOfTuplesLeft();
            case QueryCompilation::JoinBuildSideType::Right: return windowVal->getNumberOfTuplesRight();
        }
    }

    return -1;
}

void NLJOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) { NES_DEBUG("stop NLJOperatorHandler"); }

void NLJOperatorHandler::triggerWindows(std::vector<uint64_t>& windowIdentifiersToBeTriggered,
                                        WorkerContext*,
                                        PipelineExecutionContext* pipelineCtx) {

    // We expect the windowIdentifier to be the windowEnd. We have to set the seq number in order of the window end timestamps.
    // Otherwise, it can happen that the buffer <seq 2, ts 1000> gets processed after <seq 1, ts 20000>, which will lead to wrong results upstream
    std::sort(windowIdentifiersToBeTriggered.begin(), windowIdentifiersToBeTriggered.end());

    // We expect the windowIdentifiersToBeTriggered to be sorted to not
    for (auto& windowIdentifier : windowIdentifiersToBeTriggered) {
        auto nljWindow = getWindowByWindowIdentifier(windowIdentifier);
        if (!nljWindow.has_value()) {
            NES_THROW_RUNTIME_ERROR("Could not find window for " << std::to_string(windowIdentifier)
                                                                 << ". Therefore, the window will not be triggered!!!");
        }
        const auto& windowVal = nljWindow.value();
        std::dynamic_pointer_cast<NLJWindow>(windowVal)->combinePagedVectors();

        auto buffer = pipelineCtx->getBufferManager()->getBufferBlocking();
        std::memcpy(buffer.getBuffer(), &windowIdentifier, sizeof(uint64_t));
        buffer.setNumberOfTuples(1);

        // As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
        // As we have a global watermark that is larger then the window end, we can set the watermarkTs to be the window end.
        buffer.setOriginId(getOutputOriginId());
        buffer.setSequenceNumber(getNextSequenceNumber());
        buffer.setWatermark(windowVal->getWindowEnd());

        pipelineCtx->dispatchBuffer(buffer);
        NES_TRACE("Emitted windowIdentifier for window {} with watermarkTs {} sequenceNumber {} originId {}",
                  windowVal->toString(),
                  buffer.getWatermark(),
                  buffer.getSequenceNumber(),
                  buffer.getOriginId());
    }
}

NLJOperatorHandlerPtr NLJOperatorHandler::create(const std::vector<OriginId>& inputOrigins,
                                                 const OriginId outputOriginId,
                                                 const uint64_t sizeOfTupleInByteLeft,
                                                 const uint64_t sizeOfTupleInByteRight,
                                                 const uint64_t sizePageLeft,
                                                 const uint64_t rightPageSize,
                                                 const size_t windowSize) {

    return std::make_shared<NLJOperatorHandler>(inputOrigins,
                                                outputOriginId,
                                                sizeOfTupleInByteLeft,
                                                sizeOfTupleInByteRight,
                                                sizePageLeft,
                                                rightPageSize,
                                                windowSize);
}

StreamWindow* NLJOperatorHandler::getCurrentWindowOrCreate() {
    if (windows.rlock()->empty()) {
        return getWindowByTimestampOrCreateIt(0).get();
    }
    return windows.rlock()->back().get();
}

uint64_t NLJOperatorHandler::getLeftPageSize() const { return leftPageSize; }

uint64_t NLJOperatorHandler::getRightPageSize() const { return rightPageSize; }

void* getNLJPagedVectorProxy(void* ptrNljWindow, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_ASSERT2_FMT(ptrNljWindow != nullptr, "nlj window pointer should not be null!");
    auto joinBuildSide = magic_enum::enum_cast<QueryCompilation::JoinBuildSideType>(joinBuildSideInt).value();
    auto* nljWindow = static_cast<NLJWindow*>(ptrNljWindow);
    switch (joinBuildSide) {
        case QueryCompilation::JoinBuildSideType::Left: return nljWindow->getPagedVectorRefLeft(workerId);
        case QueryCompilation::JoinBuildSideType::Right: return nljWindow->getPagedVectorRefRight(workerId);
    }
}

uint64_t getNLJWindowStartProxy(void* ptrNljWindow) {
    NES_ASSERT2_FMT(ptrNljWindow != nullptr, "nlj window pointer should not be null!");
    auto* nljWindow = static_cast<NLJWindow*>(ptrNljWindow);
    return nljWindow->getWindowStart();
}

uint64_t getNLJWindowEndProxy(void* ptrNljWindow) {
    NES_ASSERT2_FMT(ptrNljWindow != nullptr, "nlj window pointer should not be null!");
    auto* nljWindow = static_cast<NLJWindow*>(ptrNljWindow);
    return nljWindow->getWindowEnd();
}
}// namespace NES::Runtime::Execution::Operators