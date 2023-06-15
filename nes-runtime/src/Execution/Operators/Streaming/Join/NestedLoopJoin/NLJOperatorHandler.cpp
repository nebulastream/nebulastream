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

void* NLJOperatorHandler::getPagedVectorRef(uint64_t timestamp, bool leftSide) {
    auto currentWindow = getWindowByTimestampOrCreateIt(timestamp);
    auto* windowPtr = static_cast<NLJWindow*>(currentWindow.get());
    return windowPtr->getPagedVectorRef(leftSide);
}

NLJOperatorHandler::NLJOperatorHandler(const std::vector<OriginId>& origins,
                                       size_t windowSize,
                                       uint64_t sizeOfTupleInByteLeft,
                                       uint64_t sizeOfTupleInByteRight)
    : StreamJoinOperatorHandler(origins,
                                windowSize,
                                NES::Runtime::Execution::StreamJoinStrategy::NESTED_LOOP_JOIN,
                                sizeOfTupleInByteLeft,
                                sizeOfTupleInByteRight) {}

void NLJOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start HashJoinOperatorHandler");
}

uint64_t NLJOperatorHandler::getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide) {
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        auto sizeOfTupleInByte = isLeftSide ? sizeOfRecordLeft : sizeOfRecordRight;
        NLJWindow* nljWindow = static_cast<NLJWindow*>(window.value().get());
        return nljWindow->getNumberOfTuples(sizeOfTupleInByte, isLeftSide);
    }

    return -1;
}

void NLJOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) { NES_DEBUG("stop HashJoinOperatorHandler"); }

void NLJOperatorHandler::triggerWindows(std::vector<uint64_t> windowIdentifiersToBeTriggered,
                                        WorkerContext* workerCtx,
                                        PipelineExecutionContext* pipelineCtx) {
    for (auto& windowIdentifier : windowIdentifiersToBeTriggered) {
        auto buffer = workerCtx->allocateTupleBuffer();
        std::memcpy(buffer.getBuffer(), &windowIdentifier, sizeof(uint64_t));
        buffer.setNumberOfTuples(1);
        pipelineCtx->dispatchBuffer(buffer);
        NES_TRACE("Emitted windowIdentifier {}", windowIdentifier);
    }
}

NLJOperatorHandlerPtr NLJOperatorHandler::create(const std::vector<OriginId>& origins,
                                                 size_t windowSize,
                                                 uint64_t sizeOfTupleInByteLeft,
                                                 uint64_t sizeOfTupleInByteRight) {

    return std::make_shared<NLJOperatorHandler>(origins, windowSize, sizeOfTupleInByteLeft, sizeOfTupleInByteRight);
}

uint8_t* NLJOperatorHandler::getFirstTuple(uint64_t windowIdentifier, bool isLeftSide) {
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        if (joinStrategy == StreamJoinStrategy::NESTED_LOOP_JOIN) {
            auto sizeOfTupleInByte = isLeftSide ? sizeOfRecordLeft : sizeOfRecordRight;
            return static_cast<NLJWindow*>(window->get())->getTuple(sizeOfTupleInByte, 0, isLeftSide);
        }
        //TODO For hash window it is not clear what this would be
    }
    return std::nullptr_t();
}
}// namespace NES::Runtime::Execution::Operators