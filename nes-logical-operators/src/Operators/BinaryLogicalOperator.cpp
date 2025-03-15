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
        auto logOp = dynamic_cast<LogicalOperator*>(child.get());
        if (!logOp->inferSchema())
        {
            throw CannotInferSchema("BinaryOperator: failed inferring the schema of the child operator");
        }
    }

    ///Identify different type of schemas from children operators
    for (const auto& child : children)
    {
        auto logOp = dynamic_cast<LogicalOperator*>(child.get());
        auto childOutputSchema = logOp->getOutputSchema();
        auto found = std::find_if(
            distinctSchemas.begin(),
            distinctSchemas.end(),
            [&](const Schema& distinctSchema) { return (childOutputSchema == distinctSchema); });
        if (found == distinctSchemas.end())
        {
            distinctSchemas.push_back(childOutputSchema);
        }
    }

    ///validate that only two different type of schema were present
    INVARIANT(distinctSchemas.size() == 2, "BinaryOperator: this node should have exactly two distinct schemas");

    return true;
}

std::vector<std::unique_ptr<Operator>> BinaryLogicalOperator::getOperatorsBySchema(const Schema& schema) const
{
    std::vector<std::unique_ptr<Operator>> operators;
    for (const auto& child : children)
    {
        auto logOp = dynamic_cast<LogicalOperator*>(child.get());
        if (logOp->getOutputSchema() == schema)
        {
            operators.emplace_back(logOp->clone());
        }
    }
    return operators;
}

std::vector<std::unique_ptr<Operator>> BinaryLogicalOperator::getLeftOperators() const
{
    return getOperatorsBySchema(getLeftInputSchema());
}

std::vector<std::unique_ptr<Operator>> BinaryLogicalOperator::getRightOperators() const
{
    return getOperatorsBySchema(getRightInputSchema());
}

void BinaryLogicalOperator::setRightInputSchema(const Schema& schema)
{
    rightInputSchema = schema;
}

Schema BinaryLogicalOperator::getRightInputSchema() const
{
    return rightInputSchema;
}

void BinaryLogicalOperator::setOutputSchema(const Schema& outputSchema)
{
    this->outputSchema = std::move(outputSchema);
}

Schema BinaryLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

void BinaryLogicalOperator::setLeftInputSchema(const Schema& schema)
{
    leftInputSchema = schema;
}

Schema BinaryLogicalOperator::getLeftInputSchema() const
{
    return leftInputSchema;
}

void BinaryLogicalOperator::setLeftInputOriginIds(const std::vector<OriginId>& originIds)
{
    leftInputOriginIds = originIds;
}

std::vector<OriginId> BinaryLogicalOperator::getLeftInputOriginIds() const
{
    return leftInputOriginIds;
}

void BinaryLogicalOperator::setRightInputOriginIds(const std::vector<OriginId>& originIds)
{
    rightInputOriginIds = originIds;
}
std::vector<OriginId> BinaryLogicalOperator::getRightInputOriginIds() const
{
    return rightInputOriginIds;
}

std::vector<OriginId> BinaryLogicalOperator::getOutputOriginIds() const
{
    std::vector<OriginId> outputOriginIds = leftInputOriginIds;
    outputOriginIds.insert(outputOriginIds.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return outputOriginIds;
}

std::vector<OriginId> BinaryLogicalOperator::getAllInputOriginIds()
{
    std::vector<OriginId> vec;
    vec.insert(vec.end(), leftInputOriginIds.begin(), leftInputOriginIds.end());
    vec.insert(vec.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return vec;
}

std::vector<LogicalOperator*> BinaryLogicalOperator::getLeftOperators()
{
    std::vector<LogicalOperator*> operators;
    for (const auto& child : children)
    {
        auto *logOp = dynamic_cast<LogicalOperator*>(child.get());
        if (logOp->getOutputSchema() == leftInputSchema)
        {
            operators.emplace_back(logOp);
        }
    }
    return operators;
}

std::vector<LogicalOperator*> BinaryLogicalOperator::getRightOperators()
{
    std::vector<LogicalOperator*> operators;
    for (const auto& child : children)
    {
        auto *logOp = dynamic_cast<LogicalOperator*>(child.get());
        if (logOp->getOutputSchema() == rightInputSchema)
        {
            operators.emplace_back(logOp);
        }
    }
    return operators;
}

void BinaryLogicalOperator::inferInputOrigins()
{
    leftInputOriginIds.clear();
    rightInputOriginIds.clear();

    for (const auto& child : this->getLeftOperators())
    {
        std::unique_ptr<Operator> cloned = child->clone();
        LogicalOperator* rawLogical = dynamic_cast<LogicalOperator*>(cloned.get());
        INVARIANT(rawLogical, "Clone did not return a LogicalOperator.");
        cloned.release();
        std::unique_ptr<LogicalOperator> childOperator(rawLogical);
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        leftInputOriginIds.insert(leftInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }

    for (const auto& child : this->getRightOperators())
    {
        std::unique_ptr<Operator> cloned = child->clone();
        LogicalOperator* rawLogical = dynamic_cast<LogicalOperator*>(cloned.get());
        INVARIANT(rawLogical, "Clone did not return a LogicalOperator.");
        cloned.release();
        std::unique_ptr<LogicalOperator> childOperator(rawLogical);
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        rightInputOriginIds.insert(rightInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
}

}
