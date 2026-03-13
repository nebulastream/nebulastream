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

#include <Operators/Sinks/SinkLogicalOperator.hpp>

#include <algorithm>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include "Util/Overloaded.hpp"

namespace NES
{

SinkLogicalOperator::SinkLogicalOperator(WeakLogicalOperator self, Identifier sinkName)
    : self(std::move(self)), sinkName(std::move(sinkName)) { };

SinkLogicalOperator::SinkLogicalOperator(WeakLogicalOperator self, const SinkDescriptor& sinkDescriptor)
    : self(std::move(self)), sinkName(sinkDescriptor.getSinkName()), sinkDescriptor(std::move(sinkDescriptor))
{
}

SinkLogicalOperator::SinkLogicalOperator(WeakLogicalOperator self, const SinkDescriptor& sinkDescriptor, LogicalOperator child)
    : self(std::move(self)), sinkName(sinkDescriptor.getSinkName()), sinkDescriptor(std::move(sinkDescriptor)), child(std::move(child))
{
}

bool SinkLogicalOperator::operator==(const SinkLogicalOperator& rhs) const
{
    const bool descriptorsEqual = (not sinkDescriptor.has_value() && not rhs.sinkDescriptor.has_value())
        || (sinkDescriptor.has_value() && rhs.sinkDescriptor.has_value() && *sinkDescriptor == *rhs.sinkDescriptor);

    return sinkName == rhs.sinkName && descriptorsEqual && getTraitSet() == rhs.getTraitSet();
}

std::string SinkLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        if (sinkDescriptor.has_value())
        {
            const auto formattedSchema = std::visit(
                Overloaded{
                    [](const auto& schemaPtr) { return fmt::format(" schema: {},", *schemaPtr); },
                    [](const std::monostate&) { return std::string{}; }},
                sinkDescriptor->getSchema());

            return fmt::format(
                "SINK(opId: {}, sinkName: {}, sinkDescriptor: {},{} traitSet: {})",
                id,
                sinkName,
                fmt::format("{}", *sinkDescriptor),
                formattedSchema,
                traitSet.explain(verbosity));
        }
        return fmt::format("SINK(opId: {}, sinkName: {})", id, sinkName);
    }
    return fmt::format("SINK({})", sinkName);
}

std::string_view SinkLogicalOperator::getName() const noexcept
{
    return NAME;
}

void SinkLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    PRECONDITION(sinkDescriptor.has_value(), "Sink descriptor not set when calling schema inference");

    auto inputSchema = child->getOutputSchema();
    auto unboundInputSchema = unbind(inputSchema);
    /// Set unordered schema for sinks not declared with a target schema.
    /// Schema<Field, Unordered> order is determined in a stage
    if (std::holds_alternative<InlineSinkDescriptor>(sinkDescriptor->underlying))
    {
        auto& inlineSinkDescriptor = std::get<InlineSinkDescriptor>(sinkDescriptor->underlying);
        if (std::holds_alternative<std::monostate>(inlineSinkDescriptor.getSchema()))
        {
            inlineSinkDescriptor.schema = std::make_shared<const Schema<UnqualifiedUnboundField, Unordered>>(
                unboundInputSchema | std::ranges::to<Schema<UnqualifiedUnboundField, Unordered>>());
        }
    }
    else
    {
        const auto expectedSchema = std::visit(
            Overloaded{
                [](const auto& schemaPtr) { return *schemaPtr | std::ranges::to<Schema<UnqualifiedUnboundField, Unordered>>(); },
                [](const std::monostate) -> Schema<UnqualifiedUnboundField, Unordered>
                { INVARIANT(false, "Schema<Field, Unordered> was not set but previous checks succeeded"); }},
            sinkDescriptor->getSchema());

        if (expectedSchema != unboundInputSchema)
        {
            std::unordered_set<UnqualifiedUnboundField> expectedButNotInInput;
            std::unordered_set<UnqualifiedUnboundField> inputButNotInExpected;
            for (const auto& field : expectedSchema)
            {
                if (!unboundInputSchema.contains(field.getFullyQualifiedName()))
                {
                    expectedButNotInInput.insert(field);
                }
                else if (unboundInputSchema[field.getFullyQualifiedName()]->getDataType() != field.getDataType())
                {
                    expectedButNotInInput.insert(field);
                }
            }
            for (const auto& field : unboundInputSchema)
            {
                if (!expectedSchema.contains(field.getFullyQualifiedName()))
                {
                    inputButNotInExpected.insert(field);
                }
                else if (expectedSchema[field.getFullyQualifiedName()]->getDataType() != field.getDataType())
                {
                    inputButNotInExpected.insert(field);
                }
            }
            throw CannotInferSchema(
                "The schema of the sink must be equal to the schema of the input operator. Expected fields {} where not found, and found "
                "unexpected fields {}",
                expectedButNotInInput | std::ranges::to<std::vector>(),
                inputButNotInExpected | std::ranges::to<std::vector>());
        }
    }
}

SinkLogicalOperator SinkLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    PRECONDITION(sinkDescriptor.has_value(), "Sink descriptor not set when calling schema inference");
    auto copy = *this;
    copy.child = child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

SinkLogicalOperator SinkLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet SinkLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SinkLogicalOperator SinkLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for sink, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

Schema<Field, Unordered> SinkLogicalOperator::getOutputSchema() const
{
    INVARIANT(false, "SinkLogicalOperator does not define a output schema");
    std::unreachable();
}

std::vector<LogicalOperator> SinkLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator SinkLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Identifier SinkLogicalOperator::getSinkName() const noexcept
{
    return sinkName;
}

std::optional<SinkDescriptor> SinkLogicalOperator::getSinkDescriptor() const
{
    return sinkDescriptor;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
SinkLogicalOperator SinkLogicalOperator::withSinkDescriptor(SinkDescriptor sinkDescriptor) const
{
    SinkLogicalOperator newOperator(*this);
    newOperator.sinkDescriptor = std::move(sinkDescriptor);
    return newOperator;
}

Reflected Reflector<TypedLogicalOperator<SinkLogicalOperator>>::operator()(const TypedLogicalOperator<SinkLogicalOperator>& op) const
{
    return reflect(detail::ReflectedSinkLogicalOperator{
        .operatorId = op.getId(), .sinkDescriptor = op->getSinkDescriptor(), .sinkName = op->getSinkName()});
}

Unreflector<TypedLogicalOperator<SinkLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<SinkLogicalOperator>
Unreflector<TypedLogicalOperator<SinkLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, descriptor, name] = context.unreflect<detail::ReflectedSinkLogicalOperator>(reflected);
    if (descriptor.has_value())
    {
        if (descriptor->getSinkName() != name)
        {
            throw CannotDeserialize(
                "SinkLogicalOperator cannot be deserialized because the sink name in the operator ({}) and the sink name in the "
                "SinkDescriptor (do not link to the same sink ({}) are not equal.",
                descriptor->getSinkName(),
                name);
        }
        auto children = plan->getChildrenFor(id, context);
        if (children.size() != 1)
        {
            throw CannotDeserialize("SinkLogicalOperator requires exactly one child, but got {}", children.size());
        }

        return TypedLogicalOperator<SinkLogicalOperator>{descriptor.value(), std::move(children.at(0))};
    }
    throw CannotDeserialize("SinkLogicalOperator requires a sink descriptor, but got none");
}
}

std::size_t std::hash<NES::SinkLogicalOperator>::operator()(const NES::SinkLogicalOperator& sinkLogicalOperator) const noexcept
{
    return folly::hash::hash_combine(sinkLogicalOperator.sinkName, sinkLogicalOperator.sinkDescriptor);
}
