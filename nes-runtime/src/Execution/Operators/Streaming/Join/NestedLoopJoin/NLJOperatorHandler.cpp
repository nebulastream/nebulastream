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
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <optional>

namespace NES::Runtime::Execution::Operators {

    uint8_t* NLJOperatorHandler::insertNewTuple(uint64_t timestamp, bool isLeftSide) {
        std::lock_guard<std::mutex> lock(insertNewTupleMutex);

        auto window = getWindowByTimestamp(timestamp);
        while(!window.has_value()) {
            createNewWindow(timestamp);
            window = getWindowByTimestamp(timestamp);
        }
        auto sizeOfTupleInByte = isLeftSide ? joinSchemaLeft->getSchemaSizeInBytes() : joinSchemaRight->getSchemaSizeInBytes();
        return window.value()->insertNewTuple(sizeOfTupleInByte, isLeftSide);
    }

    void NLJOperatorHandler::createNewWindow(uint64_t timestamp) {
        auto windowStart = sliceAssigner.getSliceStartTs(timestamp);
        auto windowEnd = sliceAssigner.getSliceEndTs(timestamp);
        nljWindows.emplace_back(windowStart, windowEnd);
    }

    void NLJOperatorHandler::deleteWindow(uint64_t windowIdentifier) {
        const auto window = getWindowByWindowIdentifier(windowIdentifier);
        if (window.has_value()) {
            nljWindows.remove(*window.value());
        }
    }

    std::optional<NLJWindow*> NLJOperatorHandler::getWindowByTimestamp(uint64_t timestamp) {
        for (auto& curWindow : nljWindows) {
            if (curWindow.getWindowStart() <= timestamp && timestamp < curWindow.getWindowEnd()) {
                return &curWindow;
            }
        }
        return std::nullopt;
    }

    uint64_t NLJOperatorHandler::getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide) {
        const auto sizeOfTupleInByte = isLeftSide ? joinSchemaLeft->getSchemaSizeInBytes() : joinSchemaRight->getSchemaSizeInBytes();
        const auto window = getWindowByWindowIdentifier(windowIdentifier);
        if (window.has_value()) {
            return window.value()->getNumberOfTuples(sizeOfTupleInByte, isLeftSide);
        }

        return -1;
    }

    uint8_t* NLJOperatorHandler::getFirstTuple(uint64_t windowIdentifier, bool isLeftSide) {
        const auto sizeOfTupleInByte = isLeftSide ? joinSchemaLeft->getSchemaSizeInBytes() : joinSchemaRight->getSchemaSizeInBytes();
        const auto window = getWindowByWindowIdentifier(windowIdentifier);
        if (window.has_value()) {
            return window.value()->getTuple(sizeOfTupleInByte, 0, isLeftSide);
        }
        return std::nullptr_t();
    }

    std::optional<NLJWindow*> NLJOperatorHandler::getWindowByWindowIdentifier(uint64_t windowIdentifier) {
        for (auto& curWindow : nljWindows) {
            if (curWindow.getWindowIdentifier() == windowIdentifier) {
                return &curWindow;
            }
        }
        return std::nullopt;
    }

    SchemaPtr NLJOperatorHandler::getSchema(bool isLeftSide) {
        if (isLeftSide) {
            return joinSchemaLeft;
        } else {
            return joinSchemaRight;
        }
    }

    std::pair<uint64_t, uint64_t> NLJOperatorHandler::getWindowStartEnd(uint64_t windowIdentifier) {
        const auto& window = getWindowByWindowIdentifier(windowIdentifier);
        if (window.has_value()) {
            return {window.value()->getWindowStart(), window.value()->getWindowEnd()};
        }
        return std::pair<uint64_t, uint64_t>();
    }

    NLJOperatorHandler::NLJOperatorHandler(size_t windowSize, const SchemaPtr &joinSchemaLeft,
                                           const SchemaPtr &joinSchemaRight, const std::string &joinFieldNameLeft,
                                           const std::string &joinFieldNameRight, const std::vector<OriginId>& origins)
                                           : sliceAssigner(windowSize, windowSize),
                                           watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)),
                                           joinSchemaLeft(joinSchemaLeft),
                                           joinSchemaRight(joinSchemaRight),
                                           joinFieldNameLeft(joinFieldNameLeft),
                                           joinFieldNameRight(joinFieldNameRight){}

    void NLJOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
        NES_DEBUG("start HashJoinOperatorHandler");
    }

    void NLJOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
        NES_DEBUG("stop HashJoinOperatorHandler");
    }

    std::vector<uint64_t> NLJOperatorHandler::checkWindowsTrigger(uint64_t watermarkTs, uint64_t sequenceNumber,
                                                                  OriginId originId) {
        std::vector<uint64_t> triggerableWindowIdentifiers;

        auto newGlobalWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);
        NES_DEBUG2("newGlobalWatermark {} watermarkTs {} sequenceNumber {} originId {}", newGlobalWatermark,
                   watermarkTs, sequenceNumber, originId);
        for (auto& window : nljWindows) {
            if (window.getWindowEnd() > newGlobalWatermark) {
                continue;
            }

            auto expected = NLJWindow::WindowState::BOTH_SIDES_FILLING;
            if (window.compareExchangeStrong(expected, NLJWindow::WindowState::EMITTED_TO_NLJ_SINK)) {
                triggerableWindowIdentifiers.emplace_back(window.getWindowIdentifier());
                NES_DEBUG2("Added window with id {} to the triggerable windows...", window.getWindowIdentifier());
            }

        }

        return triggerableWindowIdentifiers;
    }
} // namespace NES::Runtime::Execution::Operators