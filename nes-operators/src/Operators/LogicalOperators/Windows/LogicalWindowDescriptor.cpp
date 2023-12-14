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

#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Windowing {

LogicalWindowDescriptor::LogicalWindowDescriptor(const std::vector<FieldAccessExpressionNodePtr> keys,
                                                 std::vector<WindowAggregationDescriptorPtr> windowAggregation,
                                                 WindowTypePtr windowType,
                                                 DistributionCharacteristicPtr distChar,
                                                 uint64_t allowedLateness)
    : windowAggregation(std::move(windowAggregation)),
      windowType(std::move(windowType)), onKey(std::move(keys)),
      distributionType(std::move(distChar)), allowedLateness(allowedLateness) {
    NES_TRACE("LogicalWindowDescriptor: create new window definition");
}

bool LogicalWindowDescriptor::isKeyed() { return !onKey.empty(); }

LogicalWindowDefinitionPtr LogicalWindowDescriptor::create(std::vector<WindowAggregationDescriptorPtr> windowAggregations,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           uint64_t allowedLateness) {
    return create({}, windowAggregations, windowType, distChar, allowedLateness);
}

LogicalWindowDefinitionPtr LogicalWindowDescriptor::create(std::vector<FieldAccessExpressionNodePtr> keys,
                                                           std::vector<WindowAggregationDescriptorPtr> windowAggregation,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           uint64_t allowedLateness) {
    return std::make_shared<LogicalWindowDescriptor>(keys,
                                                     windowAggregation,
                                                     windowType,
                                                     distChar,
                                                     allowedLateness);
}

void LogicalWindowDescriptor::setDistributionCharacteristic(DistributionCharacteristicPtr characteristic) {
    this->distributionType = std::move(characteristic);
}

DistributionCharacteristicPtr LogicalWindowDescriptor::getDistributionType() { return distributionType; }
uint64_t LogicalWindowDescriptor::getNumberOfInputEdges() const { return numberOfInputEdges; }
void LogicalWindowDescriptor::setNumberOfInputEdges(uint64_t numberOfInputEdges) {
    this->numberOfInputEdges = numberOfInputEdges;
}
std::vector<WindowAggregationDescriptorPtr> LogicalWindowDescriptor::getWindowAggregation() { return windowAggregation; }
WindowTypePtr LogicalWindowDescriptor::getWindowType() { return windowType; }
std::vector<FieldAccessExpressionNodePtr> LogicalWindowDescriptor::getKeys() { return onKey; }
void LogicalWindowDescriptor::setWindowAggregation(std::vector<WindowAggregationDescriptorPtr> windowAggregation) {
    this->windowAggregation = std::move(windowAggregation);
}
void LogicalWindowDescriptor::setWindowType(WindowTypePtr windowType) { this->windowType = std::move(windowType); }
void LogicalWindowDescriptor::setOnKey(std::vector<FieldAccessExpressionNodePtr> onKey) { this->onKey = std::move(onKey); }

LogicalWindowDefinitionPtr LogicalWindowDescriptor::copy() {
    return create(onKey, windowAggregation, windowType, distributionType, allowedLateness);
}

WindowActionDescriptorPtr LogicalWindowDescriptor::getTriggerAction() const { return triggerAction; }

std::string LogicalWindowDescriptor::toString() {
    std::stringstream ss;
    ss << std::endl;
    ss << "windowType=" << windowType->toString();
    if (isKeyed()) {
        //ss << " onKey=" << onKey << std::endl;
    }
    ss << " distributionType=" << distributionType->toString() << std::endl;
    ss << " numberOfInputEdges=" << numberOfInputEdges;
    ss << std::endl;
    return ss.str();
}
uint64_t LogicalWindowDescriptor::getOriginId() const { return originId; }
void LogicalWindowDescriptor::setOriginId(OriginId originId) { this->originId = originId; }
uint64_t LogicalWindowDescriptor::getAllowedLateness() const { return allowedLateness; }

bool LogicalWindowDescriptor::equal(LogicalWindowDefinitionPtr otherWindowDefinition) {

    if (this->isKeyed() != otherWindowDefinition->isKeyed()) {
        return false;
    }

    if (this->getKeys().size() != otherWindowDefinition->getKeys().size()) {
        return false;
    }

    for (uint64_t i = 0; i < this->getKeys().size(); i++) {
        if (!this->getKeys()[i]->equal(otherWindowDefinition->getKeys()[i])) {
            return false;
        };
    }

    if (this->getWindowAggregation().size() != otherWindowDefinition->getWindowAggregation().size()) {
        return false;
    }

    for (uint64_t i = 0; i < this->getWindowAggregation().size(); i++) {
        if (!this->getWindowAggregation()[i]->equal(otherWindowDefinition->getWindowAggregation()[i])) {
            return false;
        };
    }

    return this->windowType->equal(otherWindowDefinition->getWindowType());
}
const std::vector<OriginId>& LogicalWindowDescriptor::getInputOriginIds() const { return inputOriginIds; }
void LogicalWindowDescriptor::setInputOriginIds(const std::vector<OriginId>& inputOriginIds) {
    LogicalWindowDescriptor::inputOriginIds = inputOriginIds;
}

}// namespace NES::Windowing