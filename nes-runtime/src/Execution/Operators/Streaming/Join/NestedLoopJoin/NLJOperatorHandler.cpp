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
#include <optional>

namespace NES::Runtime::Execution::Operators {

NLJOperatorHandler::NLJOperatorHandler(const std::vector<OriginId>& origins,
                                       uint64_t sizeOfTupleInByteLeft,
                                       uint64_t sizeOfTupleInByteRight,
                                       uint64_t sizePageLeft,
                                       uint64_t sizePageRight,
                                       size_t windowSize)
    : StreamJoinOperatorHandler(origins,
                                windowSize,
                                NES::Runtime::Execution::StreamJoinStrategy::NESTED_LOOP_JOIN,
                                sizeOfTupleInByteLeft,
                                sizeOfTupleInByteRight), leftPageSize(sizePageLeft), rightPageSize(sizePageRight) {}

void NLJOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start HashJoinOperatorHandler");
}

uint64_t NLJOperatorHandler::getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide) {
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        NLJWindow* nljWindow = static_cast<NLJWindow*>(window.value().get());
        return nljWindow->getNumberOfTuples(isLeftSide);
    }

    return -1;
}

void NLJOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) { NES_DEBUG2("stop HashJoinOperatorHandler"); }

void NLJOperatorHandler::triggerWindows(std::vector<uint64_t> windowIdentifiersToBeTriggered,
                                        WorkerContext* workerCtx,
                                        PipelineExecutionContext* pipelineCtx) {
    for (auto& windowIdentifier : windowIdentifiersToBeTriggered) {
        auto nljWindow = getWindowByWindowIdentifier(windowIdentifier);
        if (!nljWindow.has_value()) {
            NES_THROW_RUNTIME_ERROR("Could not find window for " << std::to_string(windowIdentifier) <<
                                    ". Therefore, the window will not be triggered!!!");
            continue;
        }
        std::dynamic_pointer_cast<NLJWindow>(nljWindow.value())->combinePagedVectors();

        auto buffer = workerCtx->allocateTupleBuffer();
        std::memcpy(buffer.getBuffer(), &windowIdentifier, sizeof(uint64_t));
        buffer.setNumberOfTuples(1);
        pipelineCtx->dispatchBuffer(buffer);
        NES_TRACE("Emitted windowIdentifier {}", windowIdentifier);
    }
}

NLJOperatorHandlerPtr NLJOperatorHandler::create(const std::vector<OriginId>& origins,
                                                 const uint64_t sizeOfTupleInByteLeft,
                                                 const uint64_t sizeOfTupleInByteRight,
                                                 const uint64_t sizePageLeft,
                                                 const uint64_t rightPageSize,
                                                 const size_t windowSize) {

    return std::make_shared<NLJOperatorHandler>(origins,
                                                sizeOfTupleInByteLeft,
                                                sizeOfTupleInByteRight,
                                                sizePageLeft,
                                                rightPageSize,
                                                windowSize);
}

StreamWindow* NLJOperatorHandler::getCurrentWindowOrCreate() {
    if (windows.empty()) {
        return getWindowByTimestampOrCreateIt(0).get();
    }
    return windows.back().get();
}

uint64_t NLJOperatorHandler::getLeftPageSize() const {
    return leftPageSize;
}

uint64_t NLJOperatorHandler::getRightPageSize() const {
    return rightPageSize;
}

void* getNLJPagedVectorProxy(void* ptrNljWindow, uint64_t workerId, bool isLeftSide) {
    NES_ASSERT2_FMT(ptrNljWindow != nullptr, "nlj window pointer should not be null!");
    auto* nljWindow = static_cast<NLJWindow*>(ptrNljWindow);
    return nljWindow->getPagedVectorRef(isLeftSide, workerId);
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