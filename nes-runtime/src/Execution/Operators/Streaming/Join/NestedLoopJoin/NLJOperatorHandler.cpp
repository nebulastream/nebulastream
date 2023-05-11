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

namespace NES::Runtime::Execution::Operators {
    bool NLJOperatorHandler::updateStateOfNLJWindows(uint64_t timestamp, bool isLeftSide) {
        NES_TRACE2("NLJOperatorHandler::updateStateOfNLJWindows for timestamp {} and isLeftSide {}", timestamp, isLeftSide);
        bool atLeastOneDoneFilling = false;
        for (auto& curWindow : nljWindows) {
            auto currentWindowState = curWindow.getWindowState();
            if (currentWindowState == NLJWindow::WindowState::DONE_FILLING) {
                atLeastOneDoneFilling = true;
                continue;
            }
            if (currentWindowState == NLJWindow::WindowState::EMITTED_TO_NLJ_SINK ||
                (isLeftSide && currentWindowState == NLJWindow::WindowState::ONLY_RIGHT_FILLING) ||
                (!isLeftSide && currentWindowState == NLJWindow::WindowState::ONLY_LEFT_FILLING)) {
                continue;
            }

            if (timestamp > curWindow.getWindowEnd()) {
                if ((isLeftSide && currentWindowState == NLJWindow::WindowState::ONLY_LEFT_FILLING) ||
                    (!isLeftSide && currentWindowState == NLJWindow::WindowState::ONLY_RIGHT_FILLING)) {
                    curWindow.updateWindowState(NLJWindow::WindowState::DONE_FILLING);
                    atLeastOneDoneFilling = true;
                }

                if (currentWindowState == NLJWindow::WindowState::BOTH_SIDES_FILLING) {
                    if (isLeftSide) {
                        curWindow.updateWindowState(NLJWindow::WindowState::ONLY_RIGHT_FILLING);
                    } else {
                        curWindow.updateWindowState(NLJWindow::WindowState::ONLY_LEFT_FILLING);
                    }
                }
            }
        }
        return atLeastOneDoneFilling;
    }

    std::list<NLJWindow> &NLJOperatorHandler::getAllNLJWindows() {
        return nljWindows;
    }

    uint8_t* NLJOperatorHandler::insertNewTuple(uint64_t timestamp, bool isLeftSide) {
        auto window = getWindowByTimestamp(timestamp);
        while(!window.has_value()) {
            createNewWindow();
            window = getWindowByTimestamp(timestamp);
        }
        auto sizeOfTupleInByte = isLeftSide ? joinSchemaLeft->getSchemaSizeInBytes() : joinSchemaRight->getSchemaSizeInBytes();
        return window.value()->insertNewTuple(sizeOfTupleInByte, isLeftSide);
    }

    void NLJOperatorHandler::createNewWindow() {
        nljWindows.emplace_back(windowStart, windowStart + windowSize - 1);
        windowStart += windowSize;
    }

    void NLJOperatorHandler::deleteWindow(uint64_t windowIdentifier) {
        const auto window = getWindowByWindowIdentifier(windowIdentifier);
        if (window.has_value()) {
            nljWindows.remove(*window.value());
        }
    }

    std::optional<NLJWindow*> NLJOperatorHandler::getWindowByTimestamp(uint64_t timestamp) {
        for (auto& curWindow : nljWindows) {
            if (curWindow.getWindowStart() <= timestamp && timestamp <= curWindow.getWindowEnd()) {
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

        return 0;
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
            if (curWindow.getWindowEnd() == windowIdentifier) {
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

    uint64_t NLJOperatorHandler::getPositionOfJoinKey(bool isLeftSide) {
        DefaultPhysicalTypeFactory physicalTypeFactory;
        const auto schema = getSchema(isLeftSide);
        const auto joinFieldName = isLeftSide ? joinFieldNameLeft : joinFieldNameRight;
        auto position = 0UL;

        for (auto& field : schema->fields) {
            auto const fieldName = field->getName();
            auto const fieldType = physicalTypeFactory.getPhysicalType(field->getDataType());
            if (fieldName == joinFieldName) {
                return position;
            }
            position += fieldType->size();
        }

        return position;
    }

    std::pair<uint64_t, uint64_t> NLJOperatorHandler::getWindowStartEnd(uint64_t windowIdentifier) {
        const auto& window = getWindowByWindowIdentifier(windowIdentifier);
        if (window.has_value()) {
            return {window.value()->getWindowStart(), window.value()->getWindowEnd()};
        }
        return std::pair<uint64_t, uint64_t>();
    }

    const std::string &NLJOperatorHandler::getJoinFieldName(bool isLeftSide) const {
        if (isLeftSide) {
            return joinFieldNameLeft;
        } else {
            return joinFieldNameRight;
        }
    }

    NLJOperatorHandler::NLJOperatorHandler(size_t windowSize, const SchemaPtr &joinSchemaLeft,
                                           const SchemaPtr &joinSchemaRight, const std::string &joinFieldNameLeft,
                                           const std::string &joinFieldNameRight) : windowSize(windowSize),
                                                                                    joinSchemaLeft(joinSchemaLeft),
                                                                                    joinSchemaRight(joinSchemaRight),
                                                                                    joinFieldNameLeft(
                                                                                            joinFieldNameLeft),
                                                                                    joinFieldNameRight(
                                                                                            joinFieldNameRight) {}

    void NLJOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
        NES_DEBUG("start HashJoinOperatorHandler");
    }

    void NLJOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
        NES_DEBUG("stop HashJoinOperatorHandler");
    }
} // namespace NES::Runtime::Execution::Operators