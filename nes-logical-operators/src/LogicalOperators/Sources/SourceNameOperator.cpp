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

#include <string>
#include <string_view>
#include <utility>
#include <API/Schema.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <OperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <fmt/ranges.h>
#include <LogicalOperators/Sources/SourceNameOperator.hpp>

namespace NES::Logical
{

SourceNameOperator::SourceNameOperator(std::string logicalSourceName) : logicalSourceName(std::move(logicalSourceName))
{
}

SourceNameOperator::SourceNameOperator(std::string logicalSourceName, const Schema& schema)
    : logicalSourceName(std::move(logicalSourceName)), schema(std::move(schema))
{
}

bool SourceNameOperator::operator==(const OperatorConcept& rhs) const
{
    if (auto* rhsOperator = dynamic_cast<const SourceNameOperator*>(&rhs))
    {
        return this->getSchema() == rhsOperator->getSchema() &&
                this->getName() == rhsOperator->getName() &&
               getOutputSchema() == rhsOperator->getOutputSchema() &&
               getInputSchemas() == rhsOperator->getInputSchemas() &&
               getInputOriginIds() == rhsOperator->getInputOriginIds() &&
               getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
}

Operator SourceNameOperator::withInferredSchema(std::vector<Schema>) const
{
    PRECONDITION(false, "Schema inference should happen on SourceDescriptorOperator");
}

std::string SourceNameOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SOURCE(opId: {}, name: {})", id, logicalSourceName);
    }
    std::string originIds;
    if (!inputOriginIds.empty())
    {
        originIds = fmt::format(", {}", fmt::join(inputOriginIds.begin(), inputOriginIds.end(), ", "));
    }
    return fmt::format("SOURCE({}{})", logicalSourceName, originIds);
}

void SourceNameOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
    NES_INFO("Data sources have no input origins, so inferInputOrigins is a noop.");
}

std::string_view SourceNameOperator::getName() const noexcept
{
    return "Source";
}

Schema SourceNameOperator::getSchema() const
{
    return schema;
}
Operator SourceNameOperator::withSchema(const Schema& schema) const
{
    auto copy = *this;
    copy.schema = schema;
    return copy;
}

Optimizer::TraitSet SourceNameOperator::getTraitSet() const
{
    return {};
}

Operator SourceNameOperator::withChildren(std::vector<Operator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SourceNameOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SourceNameOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> SourceNameOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> SourceNameOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

Operator SourceNameOperator::withInputOriginIds(std::vector<std::vector<OriginId>>) const
{
    PRECONDITION(false, "OriginId inference should happen on SourceDescriptorOperator");
}

Operator SourceNameOperator::withOutputOriginIds(std::vector<OriginId>) const
{
    PRECONDITION(false, "OriginId inference should happen on SourceDescriptorOperator");
}

std::vector<Operator> SourceNameOperator::getChildren() const
{
    return children;
}

std::string SourceNameOperator::getLogicalSourceName() const
{
    return logicalSourceName;
}

SerializableOperator SourceNameOperator::serialize() const
{
    PRECONDITION(false, "no serialize for SourceNameOperator defined. Serialization happens with SourceDescriptorOperator");
}

}
