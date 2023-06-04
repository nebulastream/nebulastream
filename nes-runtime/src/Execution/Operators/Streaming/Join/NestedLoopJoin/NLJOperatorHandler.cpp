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

uint8_t* NLJOperatorHandler::allocateNewEntry(uint64_t timestamp, bool isLeftSide) {
    auto currentWindow = getWindowByTimestamp(timestamp);
    while (!currentWindow.has_value()) {
        createNewWindow(timestamp);
    }
    currentWindow = getWindowByTimestamp(timestamp);

    auto sizeOfTupleInByte = isLeftSide ? joinSchemaLeft->getSchemaSizeInBytes() : joinSchemaRight->getSchemaSizeInBytes();
    NLJWindow* ptr = static_cast<NLJWindow*>(currentWindow->get());
    return ptr->allocateNewTuple(sizeOfTupleInByte, isLeftSide);
}

NLJOperatorHandler::NLJOperatorHandler(const SchemaPtr& joinSchemaLeft,
                                       const SchemaPtr& joinSchemaRight,
                                       const std::string& joinFieldNameLeft,
                                       const std::string& joinFieldNameRight,
                                       const std::vector<OriginId>& origins,
                                       size_t windowSize)
    : StreamJoinOperatorHandler(joinSchemaLeft,
                                joinSchemaRight,
                                joinFieldNameLeft,
                                joinFieldNameRight,
                                origins,
                                windowSize,
                                StreamJoinOperatorHandler::JoinType::NESTED_LOOP_JOIN) {}

void NLJOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start HashJoinOperatorHandler");
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
        NES_TRACE2("Emitted windowIdentifier {}", windowIdentifier);
    }
}

NLJOperatorHandlerPtr NLJOperatorHandler::create(const SchemaPtr& joinSchemaLeft,
                                                 const SchemaPtr& joinSchemaRight,
                                                 const std::string& joinFieldNameLeft,
                                                 const std::string& joinFieldNameRight,
                                                 const std::vector<OriginId>& origins,
                                                 size_t windowSize) {

    return std::make_shared<NLJOperatorHandler>(joinSchemaLeft,
                                                joinSchemaRight,
                                                joinFieldNameLeft,
                                                joinFieldNameRight,
                                                origins,
                                                windowSize);
}

}// namespace NES::Runtime::Execution::Operators