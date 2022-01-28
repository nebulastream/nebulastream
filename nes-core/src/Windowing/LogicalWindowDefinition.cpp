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

#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Util/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <utility>

namespace NES::Windowing {

LogicalWindowDefinition::LogicalWindowDefinition(WindowAggregationPtr windowAggregation,
                                                 WindowTypePtr windowType,
                                                 DistributionCharacteristicPtr distChar,
                                                 uint64_t numberOfInputEdges,
                                                 WindowTriggerPolicyPtr triggerPolicy,
                                                 WindowActionDescriptorPtr triggerAction,
                                                 uint64_t allowedLateness)
    : windowAggregation(std::move(windowAggregation)), triggerPolicy(std::move(triggerPolicy)),
      triggerAction(std::move(triggerAction)), windowType(std::move(windowType)), onKey(nullptr),
      distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges), allowedLateness(allowedLateness) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

LogicalWindowDefinition::LogicalWindowDefinition(FieldAccessExpressionNodePtr onKey,
                                                 WindowAggregationPtr windowAggregation,
                                                 WindowTypePtr windowType,
                                                 DistributionCharacteristicPtr distChar,
                                                 uint64_t numberOfInputEdges,
                                                 WindowTriggerPolicyPtr triggerPolicy,
                                                 WindowActionDescriptorPtr triggerAction,
                                                 uint64_t allowedLateness)
    : windowAggregation(std::move(windowAggregation)), triggerPolicy(std::move(triggerPolicy)),
      triggerAction(std::move(triggerAction)), windowType(std::move(windowType)), onKey(std::move(onKey)),
      distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges), allowedLateness(allowedLateness) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

bool LogicalWindowDefinition::isKeyed() { return onKey != nullptr; }

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(const WindowAggregationPtr& windowAggregation,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           uint64_t numberOfInputEdges,
                                                           const WindowTriggerPolicyPtr& triggerPolicy,
                                                           const WindowActionDescriptorPtr& triggerAction,
                                                           uint64_t allowedLateness) {
    return std::make_shared<LogicalWindowDefinition>(windowAggregation,
                                                     windowType,
                                                     distChar,
                                                     numberOfInputEdges,
                                                     triggerPolicy,
                                                     triggerAction,
                                                     allowedLateness);
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(ExpressionItem onKey,
                                                           const WindowAggregationPtr& windowAggregation,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           uint64_t numberOfInputEdges,
                                                           const WindowTriggerPolicyPtr& triggerPolicy,
                                                           const WindowActionDescriptorPtr& triggerAction,
                                                           uint64_t allowedLateness) {
    return std::make_shared<LogicalWindowDefinition>(onKey.getExpressionNode()->as<FieldAccessExpressionNode>(),
                                                     windowAggregation,
                                                     windowType,
                                                     distChar,
                                                     numberOfInputEdges,
                                                     triggerPolicy,
                                                     triggerAction,
                                                     allowedLateness);
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(const FieldAccessExpressionNodePtr& onKey,
                                                           const WindowAggregationPtr& windowAggregation,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           uint64_t numberOfInputEdges,
                                                           const WindowTriggerPolicyPtr& triggerPolicy,
                                                           const WindowActionDescriptorPtr& triggerAction,
                                                           uint64_t allowedLateness) {
    return std::make_shared<LogicalWindowDefinition>(onKey,
                                                     windowAggregation,
                                                     windowType,
                                                     distChar,
                                                     numberOfInputEdges,
                                                     triggerPolicy,
                                                     triggerAction,
                                                     allowedLateness);
}

void LogicalWindowDefinition::setDistributionCharacteristic(DistributionCharacteristicPtr characteristic) {
    this->distributionType = std::move(characteristic);
}

DistributionCharacteristicPtr LogicalWindowDefinition::getDistributionType() { return distributionType; }
uint64_t LogicalWindowDefinition::getNumberOfInputEdges() const { return numberOfInputEdges; }
void LogicalWindowDefinition::setNumberOfInputEdges(uint64_t numberOfInputEdges) {
    this->numberOfInputEdges = numberOfInputEdges;
}
WindowAggregationPtr LogicalWindowDefinition::getWindowAggregation() { return windowAggregation; }
WindowTypePtr LogicalWindowDefinition::getWindowType() { return windowType; }
FieldAccessExpressionNodePtr LogicalWindowDefinition::getOnKey() { return onKey; }
void LogicalWindowDefinition::setWindowAggregation(WindowAggregationPtr windowAggregation) {
    this->windowAggregation = std::move(windowAggregation);
}
void LogicalWindowDefinition::setWindowType(WindowTypePtr windowType) { this->windowType = std::move(windowType); }
void LogicalWindowDefinition::setOnKey(FieldAccessExpressionNodePtr onKey) { this->onKey = std::move(onKey); }

LogicalWindowDefinitionPtr LogicalWindowDefinition::copy() {
    return create(onKey,
                  windowAggregation->copy(),
                  windowType,
                  distributionType,
                  numberOfInputEdges,
                  triggerPolicy,
                  triggerAction,
                  allowedLateness);
}
WindowTriggerPolicyPtr LogicalWindowDefinition::getTriggerPolicy() const { return triggerPolicy; }
void LogicalWindowDefinition::setTriggerPolicy(WindowTriggerPolicyPtr triggerPolicy) {
    this->triggerPolicy = std::move(triggerPolicy);
}

WindowActionDescriptorPtr LogicalWindowDefinition::getTriggerAction() const { return triggerAction; }

std::string LogicalWindowDefinition::toString() {
    std::stringstream ss;
    ss << std::endl;
    ss << "windowType=" << windowType->toString();
    ss << " aggregation=" << windowAggregation->toString();
    ss << " triggerPolicy=" << triggerPolicy->toString() << std::endl;
    ss << " triggerAction=" << triggerAction->toString() << std::endl;
    if (isKeyed()) {
        ss << " onKey=" << onKey->toString() << std::endl;
    }
    ss << " distributionType=" << distributionType->toString() << std::endl;
    ss << " numberOfInputEdges=" << numberOfInputEdges;
    ss << std::endl;
    return ss.str();
}
uint64_t LogicalWindowDefinition::getOriginId() const { return originId; }
void LogicalWindowDefinition::setOriginId(uint64_t originId) { this->originId = originId; }
uint64_t LogicalWindowDefinition::getAllowedLateness() const { return allowedLateness; }

bool LogicalWindowDefinition::equal(LogicalWindowDefinitionPtr otherWindowDefinition) {

    if (this->isKeyed() != otherWindowDefinition->isKeyed()) {
        return false;
    }

    if (this->isKeyed() && !this->getOnKey()->equal(otherWindowDefinition->getOnKey())) {
        return false;
    }

    return this->windowType->equal(otherWindowDefinition->getWindowType())
        && this->windowAggregation->equal(otherWindowDefinition->getWindowAggregation());
}

}// namespace NES::Windowing