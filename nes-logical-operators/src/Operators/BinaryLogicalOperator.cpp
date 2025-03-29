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
#include "Operators/BinaryLogicalOperator.hpp"
#include <algorithm>
#include <memory>
#include <vector>
#include <fmt/format.h>
#include "API/Schema.hpp"
#include <ErrorHandling.hpp>
#include <Util/Common.hpp>


namespace NES
{

BinaryLogicalOperator::BinaryLogicalOperator() : Operator(), LogicalOperator()
{
}

bool BinaryLogicalOperator::inferSchema()
{
    PRECONDITION(children.size() == 2, "BinaryOperator: this node should have exactly two child operators");
    distinctSchemas.clear();

    /// Infer schema of all child operators
    for (const auto& child : children)
    {
        if (!NES::Util::as<LogicalOperator>(child)->inferSchema())
        {
            throw CannotInferSchema("BinaryOperator: failed inferring the schema of the child operator");
        }
    }

    ///Identify different type of schemas from children operators
    for (const auto& child : children)
    {
        auto childOutputSchema = NES::Util::as<LogicalOperator>(child)->getOutputSchema();
        auto found = std::find_if(
            distinctSchemas.begin(),
            distinctSchemas.end(),
            [&](const std::shared_ptr<Schema>& distinctSchema) { return (*childOutputSchema == *distinctSchema); });
        if (found == distinctSchemas.end())
        {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    ///validate that only two different type of schema were present
    INVARIANT(distinctSchemas.size() == 2, "BinaryOperator: this node should have exactly two distinct schemas");

    return true;
}

std::vector<std::shared_ptr<Operator>> BinaryLogicalOperator::getOperatorsBySchema(const std::shared_ptr<Schema>& schema) const
{
    std::vector<std::shared_ptr<Operator>> operators;
    for (const auto& child : children)
    {
        auto childOperator = NES::Util::as<LogicalOperator>(child);
        if (*childOperator->getOutputSchema() == *schema)
        {
            operators.emplace_back(childOperator);
        }
    }
    return operators;
}

std::vector<std::shared_ptr<Operator>> BinaryLogicalOperator::getLeftOperators() const
{
    return getOperatorsBySchema(getLeftInputSchema());
}

std::vector<std::shared_ptr<Operator>> BinaryLogicalOperator::getRightOperators() const
{
    return getOperatorsBySchema(getRightInputSchema());
}

void BinaryLogicalOperator::inferInputOrigins()
{
    /// in the default case we collect all input origins from the children/upstream operators
    std::vector<OriginId> leftInputOriginIds;
    for (auto child : this->getLeftOperators())
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        leftInputOriginIds.insert(leftInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->leftInputOriginIds = leftInputOriginIds;

    std::vector<OriginId> rightInputOriginIds;
    for (auto child : this->getRightOperators())
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        rightInputOriginIds.insert(rightInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->rightInputOriginIds = rightInputOriginIds;
}

}
