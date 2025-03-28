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

#include <memory>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/BinaryLogicalOperator.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <Operators/LogicalOperatorRegistry.hpp>
#include <Functions/FunctionSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

UnionLogicalOperator::UnionLogicalOperator(OperatorId id) : Operator(id), BinaryLogicalOperator(id)
{
}

bool UnionLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const UnionLogicalOperator*>(&rhs)->getId() == id;
}

bool UnionLogicalOperator::operator==(Operator const& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const UnionLogicalOperator*>(&rhs)) {
        return (*leftInputSchema == *rhsOperator->getLeftInputSchema()) && (*outputSchema == *rhsOperator->getOutputSchema());
    }
    return false;
}

std::string UnionLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "unionWith(" << id << ")";
    return ss.str();
}

bool UnionLogicalOperator::inferSchema()
{
    if (!BinaryLogicalOperator::inferSchema())
    {
        return false;
    }

    leftInputSchema->clear();
    rightInputSchema->clear();
    if (distinctSchemas.size() == 1)
    {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[0]);
    }
    else
    {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[1]);
    }

    if (!(*leftInputSchema == *rightInputSchema))
    {
        throw CannotInferSchema(
            "Found Schema mismatch for left and right schema types. Left schema {} and Right schema {}",
            leftInputSchema->toString(),
            rightInputSchema->toString());
    }

    if (leftInputSchema->getLayoutType() != rightInputSchema->getLayoutType())
    {
        throw CannotInferSchema("Left and right should have same memory layout");
    }

    ///Copy the schema of left input
    outputSchema->clear();
    outputSchema->copyFields(leftInputSchema);
    outputSchema->setLayoutType(leftInputSchema->getLayoutType());
    return true;
}

std::shared_ptr<Operator> UnionLogicalOperator::clone() const
{
    auto copy = std::make_shared<UnionLogicalOperator>(id);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

void UnionLogicalOperator::inferInputOrigins()
{
    /// in the default case we collect all input origins from the children/upstream operators
    std::vector<OriginId> combinedInputOriginIds;
    for (auto child : this->children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        combinedInputOriginIds.insert(combinedInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->leftInputOriginIds = combinedInputOriginIds;
}

std::unique_ptr<LogicalOperator> LogicalOperatorGeneratedRegistrar::deserializeUnionLogicalOperator(const SerializableOperator&)
{
    /// nothing to da as this operator has no members
    return std::make_unique<UnionLogicalOperator>(getNextOperatorId());
}

}
