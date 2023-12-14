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
#include <Operators/LogicalOperators/Windows/DistributionCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Join {

JoinDescriptor::JoinDescriptor(FieldAccessExpressionNodePtr leftJoinKeyType,
                                             FieldAccessExpressionNodePtr rightJoinKeyType,
                                             Windowing::WindowTypePtr windowType,
                                             Windowing::DistributionCharacteristicPtr distributionType,
                                             uint64_t numberOfInputEdgesLeft,
                                             uint64_t numberOfInputEdgesRight,
                                             JoinType joinType,
                                             OriginId originId)
    : leftJoinKeyType(std::move(leftJoinKeyType)), rightJoinKeyType(std::move(rightJoinKeyType)),
      leftSourceType(Schema::create()), rightSourceType(Schema::create()), outputSchema(Schema::create()),
      windowType(std::move(windowType)), distributionType(std::move(distributionType)),
      numberOfInputEdgesLeft(numberOfInputEdgesLeft), numberOfInputEdgesRight(numberOfInputEdgesRight),
      joinType(joinType), originId(originId) {

    NES_ASSERT(this->leftJoinKeyType, "Invalid left join key type");
    NES_ASSERT(this->rightJoinKeyType, "Invalid right join key type");

    NES_ASSERT(this->windowType, "Invalid window type");
    NES_ASSERT(this->numberOfInputEdgesLeft > 0, "Invalid number of left edges");
    NES_ASSERT(this->numberOfInputEdgesRight > 0, "Invalid number of right edges");
    NES_ASSERT((this->joinType == JoinType::INNER_JOIN || this->joinType == JoinType::CARTESIAN_PRODUCT), "Invalid Join Type");
}

LogicalJoinDefinitionPtr JoinDescriptor::create(const FieldAccessExpressionNodePtr& leftJoinKeyType,
                                                       const FieldAccessExpressionNodePtr& rightJoinKeyType,
                                                       const Windowing::WindowTypePtr& windowType,
                                                       const Windowing::DistributionCharacteristicPtr& distributionType,
                                                       uint64_t numberOfInputEdgesLeft,
                                                       uint64_t numberOfInputEdgesRight,
                                                       JoinType joinType) {
    return std::make_shared<Join::JoinDescriptor>(leftJoinKeyType,
                                                         rightJoinKeyType,
                                                         windowType,
                                                         distributionType,
                                                         numberOfInputEdgesLeft,
                                                         numberOfInputEdgesRight,
                                                         joinType);
}

FieldAccessExpressionNodePtr JoinDescriptor::getLeftJoinKey() { return leftJoinKeyType; }

FieldAccessExpressionNodePtr JoinDescriptor::getRightJoinKey() { return rightJoinKeyType; }

SchemaPtr JoinDescriptor::getLeftSourceType() { return leftSourceType; }

SchemaPtr JoinDescriptor::getRightSourceType() { return rightSourceType; }

Windowing::WindowTypePtr JoinDescriptor::getWindowType() { return windowType; }

Join::JoinDescriptor::JoinType JoinDescriptor::getJoinType() const { return joinType; }

Windowing::DistributionCharacteristicPtr JoinDescriptor::getDistributionType() const { return distributionType; }

uint64_t JoinDescriptor::getNumberOfInputEdgesLeft() const { return numberOfInputEdgesLeft; }

uint64_t JoinDescriptor::getNumberOfInputEdgesRight() const { return numberOfInputEdgesRight; }

void JoinDescriptor::updateSourceTypes(SchemaPtr leftSourceType, SchemaPtr rightSourceType) {
    if (leftSourceType) {
        this->leftSourceType = std::move(leftSourceType);
    }
    if (rightSourceType) {
        this->rightSourceType = std::move(rightSourceType);
    }
}

void JoinDescriptor::updateOutputDefinition(SchemaPtr outputSchema) {
    if (outputSchema) {
        this->outputSchema = std::move(outputSchema);
    }
}

SchemaPtr JoinDescriptor::getOutputSchema() const { return outputSchema; }

void JoinDescriptor::setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft) {
    JoinDescriptor::numberOfInputEdgesLeft = numberOfInputEdgesLeft;
}

void JoinDescriptor::setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight) {
    JoinDescriptor::numberOfInputEdgesRight = numberOfInputEdgesRight;
}

uint64_t JoinDescriptor::getOriginId() const { return originId; }
void JoinDescriptor::setOriginId(OriginId originId) { this->originId = originId; }

bool JoinDescriptor::equals(const JoinDescriptor& other) const {
    return leftJoinKeyType->equal(other.leftJoinKeyType) && rightJoinKeyType->equal(other.rightJoinKeyType)
        && leftSourceType->equals(other.leftSourceType) && rightSourceType->equals(other.rightSourceType)
        && outputSchema->equals(other.outputSchema) && windowType->equal(other.windowType)
        && distributionType->equals(*other.distributionType) && numberOfInputEdgesLeft == other.numberOfInputEdgesLeft
        && numberOfInputEdgesRight == other.numberOfInputEdgesRight && joinType == other.joinType && originId == other.originId;
}

};// namespace NES::Join
