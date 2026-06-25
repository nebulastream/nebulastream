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

#include <Operators/Sinks/InlineSinkLogicalOperator.hpp>

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

InlineSinkLogicalOperator::InlineSinkLogicalOperator(
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

TypedLogicalOperator<InlineSinkLogicalOperator> InlineSinkLogicalOperator::create(
    Identifier sinkType,
    std::optional<Schema<UnqualifiedUnboundField, Ordered>> schema,
    std::unordered_map<Identifier, std::string> config,
    std::unordered_map<Identifier, std::string> formatConfig)
{
    return TypedLogicalOperator<InlineSinkLogicalOperator>{
        std::move(sinkType), std::move(schema), std::move(config), std::move(formatConfig)};
}

InlineSinkLogicalOperator InlineSinkLogicalOperator::withInferredSchema()
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    std::unreachable();
}

Identifier InlineSinkLogicalOperator::getSinkType() const
{
    return sinkType;
}

std::unordered_map<Identifier, std::string> InlineSinkLogicalOperator::getSinkConfig() const
{
    return sinkConfig;
}

std::optional<Schema<UnqualifiedUnboundField, Ordered>> InlineSinkLogicalOperator::getTargetSchema() const
{
    return targetSchema;
}

std::unordered_map<Identifier, std::string> InlineSinkLogicalOperator::getFormatConfig() const
{
    return formatConfig;
}

bool InlineSinkLogicalOperator::operator==(const InlineSinkLogicalOperator& rhs) const
{
    return this->sinkType == rhs.sinkType && this->targetSchema == rhs.targetSchema && this->sinkConfig == rhs.sinkConfig
        && this->formatConfig == rhs.formatConfig;
}

std::string InlineSinkLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INLINE_SINK(opId: {}, name: {}, traitSet: {})", id, NAME, traitSet.explain(verbosity));
    }
    return fmt::format("INLINE_SINK({})", NAME);
}

std::string_view InlineSinkLogicalOperator::getName() noexcept
{
    return NAME;
}

InlineSinkLogicalOperator InlineSinkLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet InlineSinkLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InlineSinkLogicalOperator InlineSinkLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
InlineSinkLogicalOperator InlineSinkLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "Schema inference should happen on SinkLogicalOperator");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

Schema<Field, Unordered> InlineSinkLogicalOperator::getOutputSchema()
{
    INVARIANT(false, "SinkLogicalOperator does not define an output schema");
    std::unreachable();
}

std::vector<LogicalOperator> InlineSinkLogicalOperator::getChildren() const
{
    return children;
}

Reflected Reflector<TypedLogicalOperator<InlineSinkLogicalOperator>>::operator()(
    const TypedLogicalOperator<InlineSinkLogicalOperator>&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for InlineSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    std::unreachable();
}

TypedLogicalOperator<InlineSinkLogicalOperator>
Unreflector<TypedLogicalOperator<InlineSinkLogicalOperator>>::operator()(const Reflected&, const ReflectionContext&) const
{
    PRECONDITION(false, "no serialize for InlineSinkLogicalOperator defined. Serialization happens with SinkLogicalOperator");
    std::unreachable();
}

}

uint64_t std::hash<NES::InlineSinkLogicalOperator>::operator()(const NES::InlineSinkLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.getTargetSchema(), op.getSinkType(), op.getSinkConfig());
}
