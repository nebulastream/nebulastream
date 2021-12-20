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
#include <list>

namespace NES::Windowing {

LogicalWindowDefinition::LogicalWindowDefinition(WindowAggregationPtr windowAggregation,
                                                 WindowTypePtr windowType,
                                                 DistributionCharacteristicPtr distChar,
                                                 uint64_t numberOfInputEdges,
                                                 WindowTriggerPolicyPtr triggerPolicy,
                                                 WindowActionDescriptorPtr triggerAction,
                                                 uint64_t allowedLateness)
    : windowAggregation(std::move(windowAggregation)), triggerPolicy(std::move(triggerPolicy)),
      triggerAction(std::move(triggerAction)), windowType(std::move(windowType)), keyList(),
      distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges), allowedLateness(allowedLateness) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

LogicalWindowDefinition::LogicalWindowDefinition(std::list<FieldAccessExpressionNodePtr> keyList,
                                                 WindowAggregationPtr windowAggregation,
                                                 WindowTypePtr windowType,
                                                 DistributionCharacteristicPtr distChar,
                                                 uint64_t numberOfInputEdges,
                                                 WindowTriggerPolicyPtr triggerPolicy,
                                                 WindowActionDescriptorPtr triggerAction,
                                                 uint64_t allowedLateness)
    : windowAggregation(std::move(windowAggregation)), triggerPolicy(std::move(triggerPolicy)),
      triggerAction(std::move(triggerAction)), windowType(std::move(windowType)), keyList(std::move(keyList)),
      distributionType(std::move(distChar)), numberOfInputEdges(numberOfInputEdges), allowedLateness(allowedLateness) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}
// TODO check if this works like it should
bool LogicalWindowDefinition::isKeyed() { return !keyList.empty(); }

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

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(std::list<ExpressionItem> keyList,
                                                           const WindowAggregationPtr& windowAggregation,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           uint64_t numberOfInputEdges,
                                                           const WindowTriggerPolicyPtr& triggerPolicy,
                                                           const WindowActionDescriptorPtr& triggerAction,
                                                           uint64_t allowedLateness) {
    std::list<std::shared_ptr<FieldAccessExpressionNode>> fieldAccessList;
    for (std::list<ExpressionItem>::iterator it = keyList.begin(); it != keyList.end(); ++it) {
        auto keyExpression = it->getExpressionNode();
        if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
            NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
        } else {
            auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
            fieldAccessList.push_back(fieldAccess);
        }
    }
    return std::make_shared<LogicalWindowDefinition>(fieldAccessList,
                                                     windowAggregation,
                                                     windowType,
                                                     distChar,
                                                     numberOfInputEdges,
                                                     triggerPolicy,
                                                     triggerAction,
                                                     allowedLateness);
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(const std::list<FieldAccessExpressionNodePtr> keyList,
                                                           const WindowAggregationPtr& windowAggregation,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           uint64_t numberOfInputEdges,
                                                           const WindowTriggerPolicyPtr& triggerPolicy,
                                                           const WindowActionDescriptorPtr& triggerAction,
                                                           uint64_t allowedLateness) {
    return std::make_shared<LogicalWindowDefinition>(keyList,
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
std::list<FieldAccessExpressionNodePtr> LogicalWindowDefinition::getKeyList() { return keyList; }
void LogicalWindowDefinition::setWindowAggregation(WindowAggregationPtr windowAggregation) {
    this->windowAggregation = std::move(windowAggregation);
}
void LogicalWindowDefinition::setWindowType(WindowTypePtr windowType) { this->windowType = std::move(windowType); }
void LogicalWindowDefinition::setKeyList(std::list<FieldAccessExpressionNodePtr> keyList) { this->keyList = std::move(keyList); }

LogicalWindowDefinitionPtr LogicalWindowDefinition::copy() {
    return create(keyList,
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
       // ss << " onKey(s)=" << k->toString() << std::endl;
       ss << " onKey(s)=" << std::endl;
       for (std::list<FieldAccessExpressionNodePtr>::iterator it = keyList.begin(); it != keyList.end(); ++it) {
           ss << it->get()->toString() << std::endl;
       }
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

    // TODO: check if this works
    if (this->isKeyed() && !(this->getKeyList() == (otherWindowDefinition->getKeyList()))) {
        return false;
    }

    return this->windowType->equal(otherWindowDefinition->getWindowType())
        && this->windowAggregation->equal(otherWindowDefinition->getWindowAggregation());
}

}// namespace NES::Windowing