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

#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKeyType,
                                             FieldAccessExpressionNodePtr rightJoinKeyType, Windowing::WindowTypePtr windowType,
                                             Windowing::DistributionCharacteristicPtr distributionType,
                                             Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                             BaseJoinActionDescriptorPtr triggerAction, uint64_t numberOfInputEdgesLeft,
                                             uint64_t numberOfInputEdgesRight)
    : leftJoinKeyType(leftJoinKeyType), rightJoinKeyType(rightJoinKeyType), leftStreamType(nullptr), rightStreamType(nullptr),
      windowType(windowType), distributionType(distributionType), triggerPolicy(triggerPolicy), triggerAction(triggerAction),
      numberOfInputEdgesLeft(numberOfInputEdgesLeft), numberOfInputEdgesRight(numberOfInputEdgesRight), outputSchema(nullptr) {

    NES_ASSERT(this->leftJoinKeyType, "Invalid left join key type");
    NES_ASSERT(this->rightJoinKeyType, "Invalid right join key type");

    NES_ASSERT(this->windowType, "Invalid window type");
    NES_ASSERT(this->triggerPolicy, "Invalid trigger policy");
    NES_ASSERT(this->triggerAction, "Invalid trigger action");
    NES_ASSERT(this->numberOfInputEdgesLeft > 0, "Invalid number of left edges");
    NES_ASSERT(this->numberOfInputEdgesRight > 0, "Invalid number of right edges");
}

LogicalJoinDefinitionPtr
LogicalJoinDefinition::create(FieldAccessExpressionNodePtr leftJoinKeyType, FieldAccessExpressionNodePtr rightJoinKeyType,
                              Windowing::WindowTypePtr windowType, Windowing::DistributionCharacteristicPtr distributionType,
                              Windowing::WindowTriggerPolicyPtr triggerPolicy, BaseJoinActionDescriptorPtr triggerAction,
                              uint64_t numberOfInputEdgesLeft, uint64_t numberOfInputEdgesRight) {
    return std::make_shared<Join::LogicalJoinDefinition>(leftJoinKeyType, rightJoinKeyType, windowType, distributionType,
                                                         triggerPolicy, triggerAction, numberOfInputEdgesLeft,
                                                         numberOfInputEdgesRight);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getLeftJoinKey() { return leftJoinKeyType; }

FieldAccessExpressionNodePtr LogicalJoinDefinition::getRightJoinKey() { return rightJoinKeyType; }

SchemaPtr LogicalJoinDefinition::getLeftStreamType() { return leftStreamType; }

SchemaPtr LogicalJoinDefinition::getRightStreamType() { return rightStreamType; }

Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() { return windowType; }

Windowing::WindowTriggerPolicyPtr LogicalJoinDefinition::getTriggerPolicy() const { return triggerPolicy; }

Join::BaseJoinActionDescriptorPtr LogicalJoinDefinition::getTriggerAction() const { return triggerAction; }

Windowing::DistributionCharacteristicPtr LogicalJoinDefinition::getDistributionType() const { return distributionType; }

uint64_t LogicalJoinDefinition::getNumberOfInputEdgesLeft() { return numberOfInputEdgesLeft; }

uint64_t LogicalJoinDefinition::getNumberOfInputEdgesRight() { return numberOfInputEdgesRight; }

void LogicalJoinDefinition::updateStreamTypes(SchemaPtr leftStreamType, SchemaPtr rightStreamType) {
    this->leftStreamType = leftStreamType;
    this->rightStreamType = rightStreamType;
}

void LogicalJoinDefinition::updateOutputDefinition(SchemaPtr outputSchema) { this->outputSchema = outputSchema; }

SchemaPtr LogicalJoinDefinition::getOutputSchema() const { return outputSchema; }

};// namespace NES::Join
