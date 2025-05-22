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
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperator.hpp>
#include <Util/OperatorsUtil.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

UnaryOperator::UnaryOperator(OperatorId id) : Operator(id)
{
}

void UnaryOperator::setInputSchema(std::shared_ptr<Schema> inputSchema)
{
    if (inputSchema)
    {
        this->inputSchema = std::move(inputSchema);
    }
}

void UnaryOperator::setOutputSchema(std::shared_ptr<Schema> outputSchema)
{
    if (outputSchema)
    {
        this->outputSchema = std::move(outputSchema);
    }
}

std::shared_ptr<Schema> UnaryOperator::getInputSchema() const
{
    return inputSchema;
}

std::shared_ptr<Schema> UnaryOperator::getOutputSchema() const
{
    return outputSchema;
}

void UnaryOperator::setInputOriginIds(const std::vector<OriginId>& originIds)
{
    this->inputOriginIds = originIds;
}

std::vector<OriginId> UnaryOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> UnaryOperator::getOutputOriginIds() const
{
    return inputOriginIds;
}

std::ostream& UnaryOperator::toDebugString(std::ostream& os) const
{
    return os << fmt::format(
               "inputSchema: {}\n"
               "outputSchema: {}\n"
               "inputOriginIds: {}",
               inputSchema->toString(),
               outputSchema->toString(),
               fmt::join(inputOriginIds.begin(), inputOriginIds.end(), ", "));
}

std::ostream& UnaryOperator::toQueryPlanString(std::ostream& os) const
{
    return os << fmt::format("OriginIds({})", fmt::join(inputOriginIds.begin(), inputOriginIds.end(), ", "));
}

}
