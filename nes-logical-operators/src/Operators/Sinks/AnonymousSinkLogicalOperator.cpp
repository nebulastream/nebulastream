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

#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <fmt/format.h>


#include <DataTypes/Schema.hpp> /// NOLINT(misc-include-cleaner)
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

AnonymousSinkLogicalOperator::AnonymousSinkLogicalOperator(
    WeakLogicalOperator self,
    Identifier sinkType,
    std::optional<Schema<UnqualifiedUnboundField, Ordered>> schema,
    std::unordered_map<Identifier, std::string> config,
    std::unordered_map<Identifier, std::string> formatConfig)
    : ManagedByOperator(std::move(self))
    , targetSchema(std::move(schema))
    , sinkType(std::move(sinkType))
    , sinkConfig(std::move(config))
    , formatConfig(std::move(formatConfig))
{
}

TypedLogicalOperator<AnonymousSinkLogicalOperator> AnonymousSinkLogicalOperator::create(
    Identifier sinkType,
    std::optional<Schema<UnqualifiedUnboundField, Ordered>> schema,
    std::unordered_map<Identifier, std::string> config,
    std::unordered_map<Identifier, std::string> formatConfig)
{
    return TypedLogicalOperator<AnonymousSinkLogicalOperator>{
        std::move(sinkType), std::move(schema), std::move(config), std::move(formatConfig)};
}

AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withInferredSchema()
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    std::unreachable();
}

Identifier AnonymousSinkLogicalOperator::getSinkType() const
{
    return sinkType;
}

std::unordered_map<Identifier, std::string> AnonymousSinkLogicalOperator::getSinkConfig() const
{
    return sinkConfig;
}

std::optional<Schema<UnqualifiedUnboundField, Ordered>> AnonymousSinkLogicalOperator::getTargetSchema() const
{
    return targetSchema;
}

std::unordered_map<Identifier, std::string> AnonymousSinkLogicalOperator::getFormatConfig() const
{
    return formatConfig;
}

bool AnonymousSinkLogicalOperator::operator==(const AnonymousSinkLogicalOperator& rhs) const
{
    return this->sinkType == rhs.sinkType && this->targetSchema == rhs.targetSchema && this->sinkConfig == rhs.sinkConfig
        && this->formatConfig == rhs.formatConfig;
}

std::string AnonymousSinkLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ANONYMOUS_SINK(opId: {}, name: {}, traitSet: {})", id, NAME, traitSet.explain(verbosity));
    }
    return fmt::format("ANONYMOUS_SINK({})", NAME);
}

std::string_view AnonymousSinkLogicalOperator::getName() noexcept
{
    return NAME;
}

AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet AnonymousSinkLogicalOperator::getTraitSet() const
{
    return traitSet;
}

AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
AnonymousSinkLogicalOperator AnonymousSinkLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

Schema<Field, Unordered> AnonymousSinkLogicalOperator::getOutputSchema()
{
    INVARIANT(false, "SinkLogicalOperator does not define an output schema");
    std::unreachable();
}

std::vector<LogicalOperator> AnonymousSinkLogicalOperator::getChildren() const
{
    return children;
}

Reflected Reflector<TypedLogicalOperator<AnonymousSinkLogicalOperator>>::operator()(
    const TypedLogicalOperator<AnonymousSinkLogicalOperator>&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for AnonymousSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    std::unreachable();
}

TypedLogicalOperator<AnonymousSinkLogicalOperator>
Unreflector<TypedLogicalOperator<AnonymousSinkLogicalOperator>>::operator()(const Reflected&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for AnonymousSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    std::unreachable();
}

}

uint64_t std::hash<NES::AnonymousSinkLogicalOperator>::operator()(const NES::AnonymousSinkLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.getTargetSchema(), op.getSinkType(), op.getSinkConfig());
}
