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

#include <Operators/Sources/InlineSourceLogicalOperator.hpp>

#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


InlineSourceLogicalOperator InlineSourceLogicalOperator::withInferredSchema(const std::vector<Schema>&) const
{
    PRECONDITION(false, "Schema inference should happen on SourceDescriptorLogicalOperator");
    return *this;
}

std::string InlineSourceLogicalOperator::getSourceType() const
{
    return sourceType;
}

std::unordered_map<std::string, std::string> InlineSourceLogicalOperator::getSourceConfig() const
{
    return sourceConfig;
}

std::unordered_map<std::string, std::string> InlineSourceLogicalOperator::getParserConfig() const
{
    return parserConfig;
}

Schema InlineSourceLogicalOperator::getSchema() const
{
    return schema;
}

bool InlineSourceLogicalOperator::operator==(const InlineSourceLogicalOperator& rhs) const
{
    return this->sourceType == rhs.sourceType && this->schema == rhs.schema && this->parserConfig == rhs.parserConfig
        && this->sourceConfig == rhs.sourceConfig;
}

std::string InlineSourceLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INLINE_SOURCE(opId: {}, type: {} traitSet: {})", id, getSourceType(), traitSet.explain(verbosity));
    }
    return fmt::format("INLINE_SOURCE({})", getSourceType());
}

std::string_view InlineSourceLogicalOperator::getName() noexcept
{
    return NAME;
}

InlineSourceLogicalOperator InlineSourceLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet InlineSourceLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InlineSourceLogicalOperator InlineSourceLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> InlineSourceLogicalOperator::getInputSchemas() const
{
    return {schema};
};

Schema InlineSourceLogicalOperator::getOutputSchema() const
{
    return schema;
}

std::vector<LogicalOperator> InlineSourceLogicalOperator::getChildren() const
{
    return children;
}

InlineSourceLogicalOperator::InlineSourceLogicalOperator(
    std::string type,
    const Schema& schema,
    std::unordered_map<std::string, std::string> sourceConfig,
    std::unordered_map<std::string, std::string> parserConfig)
    : schema(schema), sourceType(std::move(type)), sourceConfig(std::move(sourceConfig)), parserConfig(std::move(parserConfig))
{
}

Reflected Reflector<InlineSourceLogicalOperator>::operator()(const InlineSourceLogicalOperator&) const
{
    PRECONDITION(false, "no serialize for InlineSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    throw CannotSerialize(
        "no serialize for InlineSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
}

InlineSourceLogicalOperator Unreflector<InlineSourceLogicalOperator>::operator()(const Reflected&) const
{
    PRECONDITION(false, "no serialize for InlineSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    throw CannotDeserialize(
        "no serialize for InlineSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
}

}
