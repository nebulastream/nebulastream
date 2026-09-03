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

#include <Operators/Statistic/ReservoirProbeLogicalOperator.hpp>

#include <array>
#include <cstddef>
#include <functional>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>

namespace NES
{

ReservoirProbeLogicalOperator::ReservoirProbeLogicalOperator(
    WeakLogicalOperator self, const StatisticId statisticId, std::vector<SampleField> sampleFields)
    : ManagedByOperator(std::move(self)), statisticId(statisticId), sampleFields(std::move(sampleFields))
{
}

ReservoirProbeLogicalOperator::ReservoirProbeLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, const StatisticId statisticId, std::vector<SampleField> sampleFields)
    : ManagedByOperator(std::move(self)), statisticId(statisticId), sampleFields(std::move(sampleFields)), child(std::move(child))
{
    inferLocalSchema();
}

TypedLogicalOperator<ReservoirProbeLogicalOperator>
ReservoirProbeLogicalOperator::create(const StatisticId statisticId, std::vector<SampleField> sampleFields)
{
    return TypedLogicalOperator<ReservoirProbeLogicalOperator>{statisticId, std::move(sampleFields)};
}

TypedLogicalOperator<ReservoirProbeLogicalOperator>
ReservoirProbeLogicalOperator::create(LogicalOperator child, const StatisticId statisticId, std::vector<SampleField> sampleFields)
{
    return TypedLogicalOperator<ReservoirProbeLogicalOperator>{std::move(child), statisticId, std::move(sampleFields)};
}

void ReservoirProbeLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto& inputSchema = child->getOutputSchema();

    /// The probe consumes the statistic metadata emitted by a StatisticStoreWriterLogicalOperator
    for (const auto* const requiredField : std::array{"statisticId", "statisticStart", "statisticEnd"})
    {
        if (not inputSchema.getFieldByName(Identifier::parse(requiredField)).has_value())
        {
            throw CannotInferSchema("ReservoirProbe requires the field {} in its input schema, but got: {}", requiredField, inputSchema);
        }
    }

    std::vector<UnqualifiedUnboundField> outputFields{
        UnqualifiedUnboundField{Identifier::parse("statisticStart"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("statisticEnd"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("statisticNumberOfSeenTuples"), DataType::Type::UINT64}};
    for (const auto& [name, dataType] : sampleFields)
    {
        outputFields.emplace_back(name, dataType);
    }

    const auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (not outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in probe output schema: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = outputSchemaOrCollisions.value();
}

std::string_view ReservoirProbeLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string ReservoirProbeLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "RESERVOIR PROBE(opId: {}, statisticId: {}, sampleFields: [{}])",
            id,
            statisticId,
            fmt::join(
                sampleFields | std::views::transform([](const auto& field) { return fmt::format("{}: {}", field.first, field.second); }),
                ", "));
    }
    return fmt::format("RESERVOIR PROBE(statisticId: {})", statisticId);
}

bool ReservoirProbeLogicalOperator::operator==(const ReservoirProbeLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId && sampleFields == rhs.sampleFields && outputSchema == rhs.outputSchema
        && traitSet == rhs.traitSet;
}

ReservoirProbeLogicalOperator ReservoirProbeLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet ReservoirProbeLogicalOperator::getTraitSet() const
{
    return traitSet;
}

ReservoirProbeLogicalOperator ReservoirProbeLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

ReservoirProbeLogicalOperator ReservoirProbeLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for reservoir probe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

ReservoirProbeLogicalOperator ReservoirProbeLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for reservoir probe, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> ReservoirProbeLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> ReservoirProbeLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator ReservoirProbeLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

StatisticId ReservoirProbeLogicalOperator::getStatisticId() const
{
    return statisticId;
}

const std::vector<ReservoirProbeLogicalOperator::SampleField>& ReservoirProbeLogicalOperator::getSampleFields() const
{
    return sampleFields;
}

Reflected Reflector<TypedLogicalOperator<ReservoirProbeLogicalOperator>>::operator()(
    const TypedLogicalOperator<ReservoirProbeLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedReservoirProbeLogicalOperator{
        .operatorId = op.getId(), .statisticId = op->getStatisticId(), .sampleFields = op->getSampleFields()});
}

Unreflector<TypedLogicalOperator<ReservoirProbeLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<ReservoirProbeLogicalOperator> Unreflector<TypedLogicalOperator<ReservoirProbeLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, statisticId, sampleFields] = context.unreflect<detail::ReflectedReservoirProbeLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("ReservoirProbeLogicalOperator must have exactly one child, but got {}", children.size());
    }
    return ReservoirProbeLogicalOperator::create(children.at(0), statisticId, std::move(sampleFields));
}

}

std::size_t std::hash<NES::ReservoirProbeLogicalOperator>::operator()(
    const NES::ReservoirProbeLogicalOperator& reservoirProbeLogicalOperator) const noexcept
{
    auto hash = folly::hash::hash_combine_generic(NES::Hash{}, reservoirProbeLogicalOperator.statisticId.getRawValue());
    for (const auto& [name, dataType] : reservoirProbeLogicalOperator.sampleFields)
    {
        hash = folly::hash::hash_combine_generic(NES::Hash{}, hash, name, dataType);
    }
    return hash;
}
