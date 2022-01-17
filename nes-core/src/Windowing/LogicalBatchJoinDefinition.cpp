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
#include <Windowing/LogicalBatchJoinDefinition.hpp>
#include <utility>
namespace NES::Join {

LogicalBatchJoinDefinition::LogicalBatchJoinDefinition(FieldAccessExpressionNodePtr leftJoinKeyType,
                                             FieldAccessExpressionNodePtr rightJoinKeyType,
                                             uint64_t numberOfInputEdgesLeft,
                                             uint64_t numberOfInputEdgesRight,
                                             JoinType joinType)
    : leftJoinKeyType(std::move(leftJoinKeyType)), rightJoinKeyType(std::move(rightJoinKeyType)),
      numberOfInputEdgesLeft(numberOfInputEdgesLeft),
      numberOfInputEdgesRight(numberOfInputEdgesRight), joinType(joinType) {

    NES_ASSERT(this->leftJoinKeyType, "Invalid left join key type");
    NES_ASSERT(this->rightJoinKeyType, "Invalid right join key type");

    NES_ASSERT(this->numberOfInputEdgesLeft > 0, "Invalid number of left edges");
    NES_ASSERT(this->numberOfInputEdgesRight > 0, "Invalid number of right edges");
    NES_ASSERT((this->joinType == INNER_JOIN || this->joinType == CARTESIAN_PRODUCT), "Invalid Join Type");
}

LogicalBatchJoinDefinitionPtr LogicalBatchJoinDefinition::create(const FieldAccessExpressionNodePtr& leftJoinKeyType,
                                                       const FieldAccessExpressionNodePtr& rightJoinKeyType,
                                                       uint64_t numberOfInputEdgesLeft,
                                                       uint64_t numberOfInputEdgesRight,
                                                       JoinType joinType) {
    return std::make_shared<Join::LogicalBatchJoinDefinition>(leftJoinKeyType,
                                                         rightJoinKeyType,
                                                         numberOfInputEdgesLeft,
                                                         numberOfInputEdgesRight,
                                                         joinType);
}

FieldAccessExpressionNodePtr LogicalBatchJoinDefinition::getLeftJoinKey() { return leftJoinKeyType; }

FieldAccessExpressionNodePtr LogicalBatchJoinDefinition::getRightJoinKey() { return rightJoinKeyType; }

SchemaPtr LogicalBatchJoinDefinition::getLeftStreamType() { return leftStreamType; }

SchemaPtr LogicalBatchJoinDefinition::getRightStreamType() { return rightStreamType; }

Join::LogicalBatchJoinDefinition::JoinType LogicalBatchJoinDefinition::getJoinType() const { return joinType; }

uint64_t LogicalBatchJoinDefinition::getNumberOfInputEdgesLeft() const { return numberOfInputEdgesLeft; }

uint64_t LogicalBatchJoinDefinition::getNumberOfInputEdgesRight() const { return numberOfInputEdgesRight; }

void LogicalBatchJoinDefinition::updateStreamTypes(SchemaPtr leftStreamType, SchemaPtr rightStreamType) {
    this->leftStreamType = std::move(leftStreamType);
    this->rightStreamType = std::move(rightStreamType);
}

void LogicalBatchJoinDefinition::updateOutputDefinition(SchemaPtr outputSchema) { this->outputSchema = std::move(outputSchema); }

SchemaPtr LogicalBatchJoinDefinition::getOutputSchema() const { return outputSchema; }
void LogicalBatchJoinDefinition::setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft) {
    LogicalBatchJoinDefinition::numberOfInputEdgesLeft = numberOfInputEdgesLeft;
}
void LogicalBatchJoinDefinition::setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight) {
    LogicalBatchJoinDefinition::numberOfInputEdgesRight = numberOfInputEdgesRight;
}

};// namespace NES::Join
