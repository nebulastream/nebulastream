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

#include <API/Schema.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <utility>

namespace NES::Join {

LogicalJoinDescriptor::LogicalJoinDescriptor(FieldAccessExpressionNodePtr leftJoinKeyType,
                                             FieldAccessExpressionNodePtr rightJoinKeyType,
                                             Windowing::WindowTypePtr windowType,
                                             uint64_t numberOfInputEdgesLeft,
                                             uint64_t numberOfInputEdgesRight,
                                             JoinType joinType,
                                             OriginId originId)
    : leftJoinKeyType(std::move(leftJoinKeyType)), rightJoinKeyType(std::move(rightJoinKeyType)),
      windowType(std::move(windowType)), numberOfInputEdgesLeft(numberOfInputEdgesLeft),
      numberOfInputEdgesRight(numberOfInputEdgesRight), joinType(joinType), originId(originId) {

    NES_ASSERT(this->leftJoinKeyType, "Invalid left join key type");
    NES_ASSERT(this->rightJoinKeyType, "Invalid right join key type");

    NES_ASSERT(this->windowType, "Invalid window type");
    NES_ASSERT(this->numberOfInputEdgesLeft > 0, "Invalid number of left edges");
    NES_ASSERT(this->numberOfInputEdgesRight > 0, "Invalid number of right edges");
    NES_ASSERT((this->joinType == JoinType::INNER_JOIN || this->joinType == JoinType::CARTESIAN_PRODUCT), "Invalid Join Type");
}

LogicalJoinDescriptorPtr LogicalJoinDescriptor::create(const FieldAccessExpressionNodePtr& leftJoinKeyType,
                                                       const FieldAccessExpressionNodePtr& rightJoinKeyType,
                                                       const Windowing::WindowTypePtr& windowType,
                                                       uint64_t numberOfInputEdgesLeft,
                                                       uint64_t numberOfInputEdgesRight,
                                                       JoinType joinType) {
    return std::make_shared<Join::LogicalJoinDescriptor>(leftJoinKeyType,
                                                         rightJoinKeyType,
                                                         windowType,
                                                         numberOfInputEdgesLeft,
                                                         numberOfInputEdgesRight,
                                                         joinType);
}

FieldAccessExpressionNodePtr LogicalJoinDescriptor::getLeftJoinKey() const { return leftJoinKeyType; }

FieldAccessExpressionNodePtr LogicalJoinDescriptor::getRightJoinKey() const { return rightJoinKeyType; }

SchemaPtr LogicalJoinDescriptor::getLeftSourceType() const { return leftSourceType; }

SchemaPtr LogicalJoinDescriptor::getRightSourceType() const { return rightSourceType; }

Windowing::WindowTypePtr LogicalJoinDescriptor::getWindowType() const { return windowType; }

Join::LogicalJoinDescriptor::JoinType LogicalJoinDescriptor::getJoinType() const { return joinType; }

uint64_t LogicalJoinDescriptor::getNumberOfInputEdgesLeft() const { return numberOfInputEdgesLeft; }

uint64_t LogicalJoinDescriptor::getNumberOfInputEdgesRight() const { return numberOfInputEdgesRight; }

void LogicalJoinDescriptor::updateSourceTypes(SchemaPtr leftSourceType, SchemaPtr rightSourceType) {
    if (leftSourceType) {
        this->leftSourceType = std::move(leftSourceType);
    }
    if (rightSourceType) {
        this->rightSourceType = std::move(rightSourceType);
    }
}

void LogicalJoinDescriptor::updateOutputDefinition(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}

SchemaPtr LogicalJoinDescriptor::getOutputSchema() const { return outputSchema; }

void LogicalJoinDescriptor::setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft) {
    LogicalJoinDescriptor::numberOfInputEdgesLeft = numberOfInputEdgesLeft;
}

void LogicalJoinDescriptor::setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight) {
    LogicalJoinDescriptor::numberOfInputEdgesRight = numberOfInputEdgesRight;
}

OriginId LogicalJoinDescriptor::getOriginId() const { return originId; }
void LogicalJoinDescriptor::setOriginId(OriginId originId) { this->originId = originId; }

bool LogicalJoinDescriptor::equals(const LogicalJoinDescriptor& other) const {
    return leftJoinKeyType->equal(other.leftJoinKeyType) && rightJoinKeyType->equal(other.rightJoinKeyType)
        && leftSourceType->equals(other.leftSourceType) && rightSourceType->equals(other.rightSourceType)
        && outputSchema->equals(other.outputSchema) && windowType->equal(other.windowType)
        && numberOfInputEdgesLeft == other.numberOfInputEdgesLeft && numberOfInputEdgesRight == other.numberOfInputEdgesRight
        && joinType == other.joinType && originId == other.originId;
}

};// namespace NES::Join
