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
#include <ostream>
#include <API/Schema.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Operators/AbstractOperators/Arity/BinaryOperator.hpp>
#include <Util/OperatorsUtil.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

BinaryOperator::BinaryOperator(OperatorId id) : Operator(id)
{
}

void BinaryOperator::setLeftInputSchema(std::shared_ptr<Schema> inputSchema)
{
    if (inputSchema)
    {
        this->leftInputSchema = std::move(inputSchema);
    }
}

void BinaryOperator::setRightInputSchema(std::shared_ptr<Schema> inputSchema)
{
    if (inputSchema)
    {
        this->rightInputSchema = std::move(inputSchema);
    }
}

void BinaryOperator::setOutputSchema(std::shared_ptr<Schema> outputSchema)
{
    if (outputSchema)
    {
        this->outputSchema = std::move(outputSchema);
    }
}

std::shared_ptr<Schema> BinaryOperator::getLeftInputSchema() const
{
    return leftInputSchema;
}

std::shared_ptr<Schema> BinaryOperator::getRightInputSchema() const
{
    return rightInputSchema;
}

std::shared_ptr<Schema> BinaryOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<OriginId> BinaryOperator::getLeftInputOriginIds()
{
    return leftInputOriginIds;
}

std::vector<OriginId> BinaryOperator::getAllInputOriginIds()
{
    std::vector<OriginId> vec;
    vec.insert(vec.end(), leftInputOriginIds.begin(), leftInputOriginIds.end());
    vec.insert(vec.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return vec;
}

void BinaryOperator::setLeftInputOriginIds(const std::vector<OriginId>& originIds)
{
    this->leftInputOriginIds = originIds;
}

std::vector<OriginId> BinaryOperator::getRightInputOriginIds()
{
    return rightInputOriginIds;
}

void BinaryOperator::setRightInputOriginIds(const std::vector<OriginId>& originIds)
{
    this->rightInputOriginIds = originIds;
}

std::vector<OriginId> BinaryOperator::getOutputOriginIds() const
{
    std::vector<OriginId> outputOriginIds = leftInputOriginIds;
    outputOriginIds.insert(outputOriginIds.end(), rightInputOriginIds.begin(), rightInputOriginIds.end());
    return outputOriginIds;
}

std::ostream& BinaryOperator::toDebugString(std::ostream& os) const
{
    return os << fmt::format(
               "leftInputSchema: {}\n"
               "rightInputSchema: {}\n"
               "outputSchema: {}\n"
               "distinctSchemas: {}\n"
               "leftInputOriginIds: {}\n"
               "rightInputOriginIds: {}",
               leftInputSchema->toString(),
               rightInputSchema->toString(),
               outputSchema->toString(),
               ::Util::concatenateVectorAsString(distinctSchemas),
               fmt::join(leftInputOriginIds.begin(), leftInputOriginIds.end(), ", "),
               fmt::join(rightInputOriginIds.begin(), rightInputOriginIds.end(), ", "));
}

std::ostream& BinaryOperator::toQueryPlanString(std::ostream& os) const
{
    return os << fmt::format(
               "leftOrigins({}),rightOrigins({})",
               fmt::join(leftInputOriginIds.begin(), leftInputOriginIds.end(), ", "),
               fmt::join(rightInputOriginIds.begin(), rightInputOriginIds.end(), ", "));
}

}
