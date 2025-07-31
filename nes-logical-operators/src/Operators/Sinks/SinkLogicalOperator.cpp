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
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>
#include "Util/Overloaded.hpp"

namespace NES
{

SinkLogicalOperator::SinkLogicalOperator(Identifier sinkName)
    : sinkName(std::move(sinkName)) { };

SinkLogicalOperator::SinkLogicalOperator(const SinkDescriptor& sinkDescriptor)
    : sinkName(sinkDescriptor.getSinkName()), sinkDescriptor(std::move(sinkDescriptor))
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

SinkLogicalOperator SinkLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    PRECONDITION(sinkDescriptor.has_value(), "Sink descriptor not set when calling schema inference");
    auto copy = *this;
    copy.child = child->withInferredSchema();

    auto inputSchema = copy.child->getOutputSchema();
    // UnboundOrderedSchema unboundInputSchema{
    //     inputSchema | std::views::transform([](const Field& field) { return UnboundField{field.getLastName(), field.getDataType()}; })
    //     | std::ranges::to<std::vector>()};

    auto unboundInputSchema = unbind(inputSchema);
    /// Set unordered schema for sinks not declared with a target schema.
    /// Schema order is determined in a stage
    if (std::holds_alternative<InlineSinkDescriptor>(copy.sinkDescriptor->underlying))
    {
        auto& inlineSinkDescriptor = std::get<InlineSinkDescriptor>(copy.sinkDescriptor->underlying);
        if (std::holds_alternative<std::monostate>(inlineSinkDescriptor.getSchema()))
        {
            inlineSinkDescriptor.schema = std::make_shared<const SchemaBase<UnboundFieldBase<1>, false>>(
                unboundInputSchema | std::ranges::to<SchemaBase<UnboundFieldBase<1>, false>>());
        }
    }
    else
    {
        const auto expectedSchema = std::visit(
            Overloaded{
                [](const auto& schemaPtr) { return *schemaPtr | std::ranges::to<SchemaBase<UnboundFieldBase<1>, false>>(); },
                [](const std::monostate) -> SchemaBase<UnboundFieldBase<1>, false>
                { INVARIANT(false, "Schema was not set but previous checks succeeded"); }},
            copy.sinkDescriptor->getSchema());

        if (expectedSchema != unboundInputSchema)
        {
            std::unordered_set<UnboundFieldBase<1>> expectedButNotInInput;
            std::unordered_set<UnboundFieldBase<1>> inputButNotInExpected;
            for (const auto& field : expectedSchema)
            {
                if (!inputSchema.contains(field.getFullyQualifiedName()))
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
            }
            throw CannotInferSchema(
                "The schema of the sink must be equal to the schema of the input operator. Expected fields {} where not found, and found "
                "unexpected fields {}",
                expectedButNotInInput | std::ranges::to<std::vector>(),
                inputButNotInExpected | std::ranges::to<std::vector>());
        }
    }

    return copy;

    // if (sinkDescriptor.has_value() && sinkDescriptor.value().isInline() && std::ranges::empty(*sinkDescriptor.value().getSchema()))
    // {
    //     copy.sinkDescriptor->schema = std::make_shared<const SchemaBase<UnboundFieldBase<1>, true>>(unboundInputSchema);
    // }
    // else if (copy.sinkDescriptor.has_value() && *copy.sinkDescriptor->getSchema() != unboundInputSchema)
    // {
    //     std::vector expectedFields(copy.sinkDescriptor.value().getSchema()->begin(), copy.sinkDescriptor.value().getSchema()->end());
    //     std::vector<UnboundField> actualFields = unboundInputSchema | std::ranges::to<std::vector>();
    //
    //     std::stringstream expectedFieldsString;
    //     std::stringstream actualFieldsString;
    //
    //     for (unsigned int i = 0; i < expectedFields.size(); ++i)
    //     {
    //         const auto& field = expectedFields.at(i);
    //         auto foundIndex = std::ranges::find(actualFields, field);
    //
    //         if (foundIndex == actualFields.end())
    //         {
    //             expectedFieldsString << field << ", ";
    //         }
    //         else if (auto foundOffset = foundIndex - std::ranges::begin(actualFields); foundOffset != i)
    //         {
    //             expectedFieldsString << fmt::format("Field {} at {}, but was at {},", field, i, foundOffset);
    //         }
    //     }
    //     for (const auto& field : actualFields)
    //     {
    //         if (std::ranges::find(expectedFields, field) == expectedFields.end())
    //         {
    //             actualFieldsString << fmt::format("UnboundField(name: {}, type: {})", field.getFullyQualifiedName(), field.getDataType())
    //                                << ", ";
    //         }
    //     }
    //
    //     throw CannotInferSchema(
    //         "The schema of the sink must be equal to the schema of the input operator. Expected fields {} where not found, and found "
    //         "unexpected fields {}",
    //         expectedFieldsString.str(),
    //         actualFieldsString.str().substr(0, actualFieldsString.str().size() - 2));
    // }
    // return copy;
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

Schema SinkLogicalOperator::getOutputSchema() const
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

void SinkLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableSinkLogicalOperator proto;
    if (sinkDescriptor)
    {
        proto.mutable_sinkdescriptor()->CopyFrom(sinkDescriptor->serialize());
    }

    const DescriptorConfig::ConfigType timeVariant = sinkName;
    (*serializableOperator.mutable_config())[ConfigParameters::SINK_NAME] = descriptorConfigTypeToProto(timeVariant);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    serializableOperator.mutable_sink()->CopyFrom(proto);
}
}

std::size_t std::hash<NES::SinkLogicalOperator>::operator()(const NES::SinkLogicalOperator& sinkLogicalOperator) const noexcept
{
    return folly::hash::hash_combine(sinkLogicalOperator.sinkName, sinkLogicalOperator.sinkDescriptor);
}