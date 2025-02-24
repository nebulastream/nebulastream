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
#include "Operators/UnaryLogicalOperator.hpp"
#include <memory>
#include "API/Schema.hpp"
#include <ErrorHandling.hpp>
#include "Util/Common.hpp"
#include "Util/Logger/Logger.hpp"

namespace NES
{

UnaryLogicalOperator::UnaryLogicalOperator() : Operator(), LogicalOperator(), inputSchema(Schema::create())
{
}

bool UnaryLogicalOperator::inferSchema()
{
    /// We assume that all children operators have the same output schema otherwise this plan is not valid
    for (const auto& child : children)
    {
        if (!NES::Util::as<LogicalOperator>(child)->inferSchema())
        {
            return false;
        }
    }

    if (children.empty())
    {
        throw CannotInferSchema("This operator ({}) should have at least one child operator", this->toString());
    }

    const auto childSchema = NES::Util::as<Operator>(children[0])->outputSchema;
    for (const auto& child : children)
    {
        if (!(*NES::Util::as<Operator>(child)->outputSchema == *childSchema))
        {
            NES_ERROR(
                "UnaryOperator: infer schema failed. The schema has to be the same across all child operators."
                "this op schema= {} child schema={}",
                NES::Util::as<Operator>(child)->outputSchema->toString(),
                childSchema->toString());
            return false;
        }
    }

    ///Reset and reinitialize the input and output schemas
    inputSchema->clear();
    inputSchema = inputSchema->copyFields(childSchema);
    inputSchema->setLayoutType(childSchema->getLayoutType());
    outputSchema->clear();
    outputSchema = outputSchema->copyFields(childSchema);
    outputSchema->setLayoutType(childSchema->getLayoutType());
    return true;
}

void UnaryLogicalOperator::inferInputOrigins()
{
    /// in the default case we collect all input origins from the children/upstream operators
    std::vector<OriginId> inputOriginIds;
    for (auto child : this->children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        inputOriginIds.insert(inputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->inputOriginIds = inputOriginIds;
}

}
