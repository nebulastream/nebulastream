/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

LogicalWindowDefinition::LogicalWindowDefinition(WindowAggregationPtr windowAggregation, WindowTypePtr windowType,
                                                 DistributionCharacteristicPtr distChar, uint64_t numberOfInputEdges,
                                                 WindowTriggerPolicyPtr triggerPolicy, WindowActionDescriptorPtr triggerAction)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(nullptr),
      distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges), triggerPolicy(std::move(triggerPolicy)),
      triggerAction(triggerAction) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

LogicalWindowDefinition::LogicalWindowDefinition(FieldAccessExpressionNodePtr onKey, WindowAggregationPtr windowAggregation,
                                                 WindowTypePtr windowType, DistributionCharacteristicPtr distChar,
                                                 uint64_t numberOfInputEdges, WindowTriggerPolicyPtr triggerPolicy,
                                                 WindowActionDescriptorPtr triggerAction)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(std::move(onKey)),
      distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges), triggerPolicy(std::move(triggerPolicy)),
      triggerAction(triggerAction) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

bool LogicalWindowDefinition::isKeyed() { return onKey != nullptr; }

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(WindowAggregationPtr windowAggregation, WindowTypePtr windowType,
                                                           DistributionCharacteristicPtr distChar, uint64_t numberOfInputEdges,
                                                           WindowTriggerPolicyPtr triggerPolicy,
                                                           WindowActionDescriptorPtr triggerAction) {
    return std::make_shared<LogicalWindowDefinition>(windowAggregation, windowType, distChar, numberOfInputEdges, triggerPolicy,
                                                     triggerAction);
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(ExpressionItem onKey, WindowAggregationPtr windowAggregation,
                                                           WindowTypePtr windowType, DistributionCharacteristicPtr distChar,
                                                           uint64_t numberOfInputEdges, WindowTriggerPolicyPtr triggerPolicy,
                                                           WindowActionDescriptorPtr triggerAction) {
    return std::make_shared<LogicalWindowDefinition>(onKey.getExpressionNode()->as<FieldAccessExpressionNode>(),
                                                     windowAggregation, windowType, distChar, numberOfInputEdges, triggerPolicy,
                                                     triggerAction);
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(FieldAccessExpressionNodePtr onKey,
                                                           WindowAggregationPtr windowAggregation, WindowTypePtr windowType,
                                                           DistributionCharacteristicPtr distChar, uint64_t numberOfInputEdges,
                                                           WindowTriggerPolicyPtr triggerPolicy,
                                                           WindowActionDescriptorPtr triggerAction) {
    return std::make_shared<LogicalWindowDefinition>(onKey, windowAggregation, windowType, distChar, numberOfInputEdges,
                                                     triggerPolicy, triggerAction);
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
    return create(onKey, windowAggregation->copy(), windowType, distributionType, numberOfInputEdges, triggerPolicy,
                  triggerAction);
}
WindowTriggerPolicyPtr LogicalWindowDefinition::getTriggerPolicy() const { return triggerPolicy; }
void LogicalWindowDefinition::setTriggerPolicy(WindowTriggerPolicyPtr triggerPolicy) { this->triggerPolicy = triggerPolicy; }

WindowActionDescriptorPtr LogicalWindowDefinition::getTriggerAction() const { return triggerAction; }

std::string LogicalWindowDefinition::toString() {
    std::stringstream ss;
    ss << "windowType=" << windowType->toString();
    ss << " aggregation=" << windowAggregation->toString();
    ss << " triggerPolicy=" << triggerPolicy->toString();
    ss << " triggerAction=" << triggerAction->toString();
    ss << " onKey=" << onKey->toString();
    ss << " distributionType=" << distributionType->toString();
    ss << " numberOfInputEdges=" << numberOfInputEdges;
    ss << std::endl;
    return ss.str();
}

//FieldAccessExpressionNodePtr onKey;
//DistributionCharacteristicPtr distributionType;
//uint64_t numberOfInputEdges;
}// namespace NES::Windowing