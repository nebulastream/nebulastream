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

#include <Operators/Sources/AnonymousSourceLogicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <folly/hash/Hash.h>
/// NOLINTNEXTLINE(misc-include-cleaner
#include <Util/Hash.hpp>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withInferredSchema()
{
    PRECONDITION(false, "Schema<Field, Unordered> inference should happen on SourceDescriptorLogicalOperator");
    std::unreachable();
}

Identifier AnonymousSourceLogicalOperator::getSourceType() const
{
    return sourceType;
}

std::unordered_map<Identifier, std::string> AnonymousSourceLogicalOperator::getSourceConfig() const
{
    return sourceConfig;
}

std::unordered_map<Identifier, std::string> AnonymousSourceLogicalOperator::getParserConfig() const
{
    return parserConfig;
}

Schema<UnqualifiedUnboundField, Ordered> AnonymousSourceLogicalOperator::getSourceSchema() const
{
    return sourceSchema;
}

bool AnonymousSourceLogicalOperator::operator==(const AnonymousSourceLogicalOperator& rhs) const
{
    return this->sourceType == rhs.sourceType && this->sourceSchema == rhs.sourceSchema && this->parserConfig == rhs.parserConfig
        && this->sourceConfig == rhs.sourceConfig;
}

std::string AnonymousSourceLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ANONYMOUS_SOURCE(opId: {}, type: {} traitSet: {})", id, getSourceType(), traitSet.explain(verbosity));
    }
    return fmt::format("ANONYMOUS_SOURCE({})", getSourceType());
}

std::string_view AnonymousSourceLogicalOperator::getName() noexcept
{
    return NAME;
}

AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet AnonymousSourceLogicalOperator::getTraitSet() const
{
    return traitSet;
}

AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
AnonymousSourceLogicalOperator AnonymousSourceLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "Schema inference should happen on SourceDescriptorLogicalOperator");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

Schema<Field, Unordered> AnonymousSourceLogicalOperator::getOutputSchema()
{
    INVARIANT(false, "Convert AnonymousSourceLogical Operator to SourceDescriptorLogicalOperator before retrieving output schema");
    std::unreachable();
}

std::vector<LogicalOperator> AnonymousSourceLogicalOperator::getChildren() const
{
    return children;
}

AnonymousSourceLogicalOperator::AnonymousSourceLogicalOperator(
    WeakLogicalOperator self,
    Identifier type,
    Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
    std::unordered_map<Identifier, std::string> sourceConfig,
    std::unordered_map<Identifier, std::string> parserConfig)
    : ManagedByOperator(std::move(self))
    , sourceSchema(std::move(sourceSchema))
    , sourceType(std::move(type))
    , sourceConfig(std::move(sourceConfig))
    , parserConfig(std::move(parserConfig))
{
}

TypedLogicalOperator<AnonymousSourceLogicalOperator> AnonymousSourceLogicalOperator::create(
    Identifier type,
    Schema<UnqualifiedUnboundField, Ordered> sourceSchema,
    std::unordered_map<Identifier, std::string> sourceConfig,
    std::unordered_map<Identifier, std::string> parserConfig)
{
    return TypedLogicalOperator<AnonymousSourceLogicalOperator>{
        std::move(type), std::move(sourceSchema), std::move(sourceConfig), std::move(parserConfig)};
}

Reflected Reflector<TypedLogicalOperator<AnonymousSourceLogicalOperator>>::operator()(
    const TypedLogicalOperator<AnonymousSourceLogicalOperator>&, const ReflectionContext&) const
{
    PRECONDITION(
        false, "no serialize for AnonymousSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

TypedLogicalOperator<AnonymousSourceLogicalOperator>
Unreflector<TypedLogicalOperator<AnonymousSourceLogicalOperator>>::operator()(const Reflected&, const ReflectionContext&) const
{
    PRECONDITION(
        false, "no serialize for AnonymousSourceLogicalOperator defined. Serialization happens with SourceDescriptorLogicalOperator");
    std::unreachable();
}

}

uint64_t std::hash<NES::AnonymousSourceLogicalOperator>::operator()(const NES::AnonymousSourceLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(
        NES::Hash{}, op.getSourceType(), op.getSourceSchema(), op.getSourceConfig(), op.getParserConfig());
}
