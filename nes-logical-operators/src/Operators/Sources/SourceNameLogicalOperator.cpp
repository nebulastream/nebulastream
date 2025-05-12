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
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Sources/SourceNameLogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName) : logicalSourceName(std::move(logicalSourceName))
{
}

SourceNameLogicalOperator::SourceNameLogicalOperator(std::string logicalSourceName, Schema schema)
    : logicalSourceName(std::move(logicalSourceName)), schema(std::move(schema))
{
}

bool SourceNameLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* rhsOperator = dynamic_cast<const SourceNameLogicalOperator*>(&rhs))
    {
        return this->getSchema() == rhsOperator->getSchema() && this->getName() == rhsOperator->getName()
            && getOutputSchema() == rhsOperator->getOutputSchema() && getInputSchemas() == rhsOperator->getInputSchemas()
            && getInputOriginIds() == rhsOperator->getInputOriginIds() && getOutputOriginIds() == rhsOperator->getOutputOriginIds()
            && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
}

LogicalOperator SourceNameLogicalOperator::withInferredSchema(std::vector<Schema>) const
{
    PRECONDITION(false, "Schema inference should happen on SourceDescriptorLogicalOperator");
    return *this;
}

std::string SourceNameLogicalOperator::explain(ExplainVerbosity verbosity) const
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

void SourceNameLogicalOperator::inferInputOrigins()
{
    /// Data sources have no input origins.
    NES_INFO("Data sources have no input origins, so inferInputOrigins is a noop.");
}

std::string_view SourceNameLogicalOperator::getName() const noexcept
{
    return "Source";
}

Schema SourceNameLogicalOperator::getSchema() const
{
    return schema;
}

LogicalOperator SourceNameLogicalOperator::withSchema(const Schema& schema) const
{
    auto copy = *this;
    copy.schema = schema;
    return copy;
}

LogicalOperator SourceNameLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet SourceNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator SourceNameLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> SourceNameLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SourceNameLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> SourceNameLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> SourceNameLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator SourceNameLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>>) const
{
    PRECONDITION(false, "OriginId inference should happen on SourceDescriptorLogicalOperator");
    return *this;
}

LogicalOperator SourceNameLogicalOperator::withOutputOriginIds(std::vector<OriginId>) const
{
    PRECONDITION(false, "OriginId inference should happen on SourceDescriptorLogicalOperator");
    return *this;
}

std::vector<LogicalOperator> SourceNameLogicalOperator::getChildren() const
{
    return children;
}

std::string SourceNameLogicalOperator::getLogicalSourceName() const
{
    return logicalSourceName;
}

SerializableOperator SourceNameLogicalOperator::serialize() const
{
    PRECONDITION(false, "no serialize for SourceNameLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    return {};
}

}
