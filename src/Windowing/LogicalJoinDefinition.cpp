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

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr joinKeyType, FieldAccessExpressionNodePtr leftStreamType,
                                             FieldAccessExpressionNodePtr rightStreamType, Windowing::WindowTypePtr windowType,
                                             Windowing::DistributionCharacteristicPtr distributionType,
                                             Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                             BaseJoinActionDescriptorPtr triggerAction, uint64_t numberOfInputEdgesLeft,
                                             uint64_t numberOfInputEdgesRight)
    : joinKeyType(joinKeyType), leftStreamType(leftStreamType), rightStreamType(rightStreamType),
      windowType(windowType), distributionType(distributionType), triggerPolicy(triggerPolicy),
      triggerAction(triggerAction), numberOfInputEdgesLeft(numberOfInputEdgesLeft),
      numberOfInputEdgesRight(numberOfInputEdgesRight) {

    NES_ASSERT(this->joinKeyType, "Invalid join key type");
#if 1
    this->leftStreamType = this->rightStreamType = this->joinKeyType;
#else
    NES_ASSERT(this->leftStreamType, "Invalid left stream type");
    NES_ASSERT(this->rightStreamType, "Invalid right stream type");
#endif

    NES_ASSERT(this->windowType, "Invalid window type");
    NES_ASSERT(this->triggerPolicy, "Invalid trigger policy");
    NES_ASSERT(this->triggerAction, "Invalid trigger action");
    NES_ASSERT(this->numberOfInputEdgesLeft > 0, "Invalid number of left edges");
    NES_ASSERT(this->numberOfInputEdgesRight > 0, "Invalid number of right edges");
}

LogicalJoinDefinitionPtr LogicalJoinDefinition::create(FieldAccessExpressionNodePtr joinKeyType, FieldAccessExpressionNodePtr leftStreamType,
                                                       FieldAccessExpressionNodePtr rightStreamType, Windowing::WindowTypePtr windowType,
                                                       Windowing::DistributionCharacteristicPtr distributionType,
                                                       Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                                       BaseJoinActionDescriptorPtr triggerAction, uint64_t numberOfInputEdgesLeft,
                                                       uint64_t numberOfInputEdgesRight) {
    return std::make_shared<Join::LogicalJoinDefinition>(joinKeyType, leftStreamType, rightStreamType, windowType,
                                                         distributionType, triggerPolicy, triggerAction,
                                                         numberOfInputEdgesLeft, numberOfInputEdgesRight);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getJoinKey() { return joinKeyType; }

FieldAccessExpressionNodePtr LogicalJoinDefinition::getLeftStreamType() { return leftStreamType; }

FieldAccessExpressionNodePtr LogicalJoinDefinition::getRightStreamType() { return rightStreamType; }

Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() { return windowType; }

Windowing::WindowTriggerPolicyPtr LogicalJoinDefinition::getTriggerPolicy() const { return triggerPolicy; }

Join::BaseJoinActionDescriptorPtr LogicalJoinDefinition::getTriggerAction() const { return triggerAction; }

Windowing::DistributionCharacteristicPtr LogicalJoinDefinition::getDistributionType() const { return distributionType; }

uint64_t LogicalJoinDefinition::getNumberOfInputEdgesLeft() { return numberOfInputEdgesLeft; }

uint64_t LogicalJoinDefinition::getNumberOfInputEdgesRight() { return numberOfInputEdgesRight; }

};// namespace NES::Join
