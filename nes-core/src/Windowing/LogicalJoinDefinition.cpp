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

#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <utility>
namespace NES::Join {

LogicalJoinDefinition::LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKeyType,
                                             FieldAccessExpressionNodePtr rightJoinKeyType,
                                             Windowing::WindowTypePtr windowType,
                                             Windowing::DistributionCharacteristicPtr distributionType,
                                             Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                             BaseJoinActionDescriptorPtr triggerAction,
                                             uint64_t numberOfInputEdgesLeft,
                                             uint64_t numberOfInputEdgesRight,
                                             JoinType joinType)
    : leftJoinKeyType(std::move(leftJoinKeyType)), rightJoinKeyType(std::move(rightJoinKeyType)),
      triggerPolicy(std::move(triggerPolicy)), triggerAction(std::move(triggerAction)), windowType(std::move(windowType)),
      distributionType(std::move(distributionType)), numberOfInputEdgesLeft(numberOfInputEdgesLeft),
      numberOfInputEdgesRight(numberOfInputEdgesRight), joinType(joinType) {

    NES_ASSERT(this->leftJoinKeyType, "Invalid left join key type");
    NES_ASSERT(this->rightJoinKeyType, "Invalid right join key type");

    NES_ASSERT(this->windowType, "Invalid window type");
    NES_ASSERT(this->triggerPolicy, "Invalid trigger policy");
    NES_ASSERT(this->triggerAction, "Invalid trigger action");
    NES_ASSERT(this->numberOfInputEdgesLeft > 0, "Invalid number of left edges");
    NES_ASSERT(this->numberOfInputEdgesRight > 0, "Invalid number of right edges");
    NES_ASSERT((this->joinType == INNER_JOIN || this->joinType == CARTESIAN_PRODUCT), "Invalid Join Type");
}

LogicalJoinDefinitionPtr LogicalJoinDefinition::create(const FieldAccessExpressionNodePtr& leftJoinKeyType,
                                                       const FieldAccessExpressionNodePtr& rightJoinKeyType,
                                                       const Windowing::WindowTypePtr& windowType,
                                                       const Windowing::DistributionCharacteristicPtr& distributionType,
                                                       const Windowing::WindowTriggerPolicyPtr& triggerPolicy,
                                                       const BaseJoinActionDescriptorPtr& triggerAction,
                                                       uint64_t numberOfInputEdgesLeft,
                                                       uint64_t numberOfInputEdgesRight,
                                                       JoinType joinType) {
    return std::make_shared<Join::LogicalJoinDefinition>(leftJoinKeyType,
                                                         rightJoinKeyType,
                                                         windowType,
                                                         distributionType,
                                                         triggerPolicy,
                                                         triggerAction,
                                                         numberOfInputEdgesLeft,
                                                         numberOfInputEdgesRight,
                                                         joinType);
}

FieldAccessExpressionNodePtr LogicalJoinDefinition::getLeftJoinKey() { return leftJoinKeyType; }

FieldAccessExpressionNodePtr LogicalJoinDefinition::getRightJoinKey() { return rightJoinKeyType; }

SchemaPtr LogicalJoinDefinition::getLeftStreamType() { return leftStreamType; }

SchemaPtr LogicalJoinDefinition::getRightStreamType() { return rightStreamType; }

Windowing::WindowTypePtr LogicalJoinDefinition::getWindowType() { return windowType; }

Windowing::WindowTriggerPolicyPtr LogicalJoinDefinition::getTriggerPolicy() const { return triggerPolicy; }

Join::BaseJoinActionDescriptorPtr LogicalJoinDefinition::getTriggerAction() const { return triggerAction; }

Join::LogicalJoinDefinition::JoinType LogicalJoinDefinition::getJoinType() const { return joinType; }

Windowing::DistributionCharacteristicPtr LogicalJoinDefinition::getDistributionType() const { return distributionType; }

uint64_t LogicalJoinDefinition::getNumberOfInputEdgesLeft() const { return numberOfInputEdgesLeft; }

uint64_t LogicalJoinDefinition::getNumberOfInputEdgesRight() const { return numberOfInputEdgesRight; }

void LogicalJoinDefinition::updateStreamTypes(SchemaPtr leftStreamType, SchemaPtr rightStreamType) {
    this->leftStreamType = std::move(leftStreamType);
    this->rightStreamType = std::move(rightStreamType);
}

void LogicalJoinDefinition::updateOutputDefinition(SchemaPtr outputSchema) { this->outputSchema = std::move(outputSchema); }

SchemaPtr LogicalJoinDefinition::getOutputSchema() const { return outputSchema; }
void LogicalJoinDefinition::setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft) {
    LogicalJoinDefinition::numberOfInputEdgesLeft = numberOfInputEdgesLeft;
}
void LogicalJoinDefinition::setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight) {
    LogicalJoinDefinition::numberOfInputEdgesRight = numberOfInputEdgesRight;
}

};// namespace NES::Join
