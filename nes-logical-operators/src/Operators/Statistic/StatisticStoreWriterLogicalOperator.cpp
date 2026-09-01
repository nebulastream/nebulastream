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

#include <Operators/Statistic/StatisticStoreWriterLogicalOperator.hpp>

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
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/Trait.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    WeakLogicalOperator self, const StatisticId statisticId, std::string typeName)
    : ManagedByOperator(std::move(self)), statisticId(statisticId), typeName(std::move(typeName))
{
}

StatisticStoreWriterLogicalOperator::StatisticStoreWriterLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, const StatisticId statisticId, std::string typeName)
    : ManagedByOperator(std::move(self)), statisticId(statisticId), typeName(std::move(typeName)), child(std::move(child))
{
    inferLocalSchema();
}

TypedLogicalOperator<StatisticStoreWriterLogicalOperator>
StatisticStoreWriterLogicalOperator::create(const StatisticId statisticId, std::string typeName)
{
    return TypedLogicalOperator<StatisticStoreWriterLogicalOperator>{statisticId, std::move(typeName)};
}

TypedLogicalOperator<StatisticStoreWriterLogicalOperator>
StatisticStoreWriterLogicalOperator::create(LogicalOperator child, const StatisticId statisticId, std::string typeName)
{
    return TypedLogicalOperator<StatisticStoreWriterLogicalOperator>{std::move(child), statisticId, std::move(typeName)};
}

void StatisticStoreWriterLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto& inputSchema = child->getOutputSchema();

    /// The writer consumes the output schema of a StatisticBuildLogicalOperator.
    /// The data field name is per-statisticId (e.g. STATISTICDATA_1).
    const auto dataFieldName = statisticDataFieldName(statisticId);
    for (const auto& requiredField :
         std::array<std::string, 4>{"STATISTICSTART", "STATISTICEND", dataFieldName, "STATISTICNUMBEROFSEENMEASUREMENTS"})
    {
        if (not inputSchema.getFieldByName(Identifier::parse(requiredField)).has_value())
        {
            throw CannotInferSchema(
                "StatisticStoreWriter requires the field {} in its input schema, but got: {}", requiredField, inputSchema);
        }
    }

    const auto outputFields = std::vector{
        UnqualifiedUnboundField{Identifier::parse("statisticId"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("statisticStart"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("statisticEnd"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("statisticNumberOfSeenMeasurements"), DataType::Type::UINT64}};
    const auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    INVARIANT(outputSchemaOrCollisions.has_value(), "The fixed statistic store writer output fields must not collide");
    outputSchema = outputSchemaOrCollisions.value();
}

std::string_view StatisticStoreWriterLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string StatisticStoreWriterLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("STATISTIC STORE WRITER(opId: {}, statisticId: {}, typeName: {})", id, statisticId, typeName);
    }
    return fmt::format("STATISTIC STORE WRITER(statisticId: {})", statisticId);
}

bool StatisticStoreWriterLogicalOperator::operator==(const StatisticStoreWriterLogicalOperator& rhs) const
{
    return statisticId == rhs.statisticId && typeName == rhs.typeName && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet StatisticStoreWriterLogicalOperator::getTraitSet() const
{
    return traitSet;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for statistic store writer, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticStoreWriterLogicalOperator StatisticStoreWriterLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for statistic store writer, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> StatisticStoreWriterLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> StatisticStoreWriterLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator StatisticStoreWriterLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

StatisticId StatisticStoreWriterLogicalOperator::getStatisticId() const
{
    return statisticId;
}

const std::string& StatisticStoreWriterLogicalOperator::getTypeName() const
{
    return typeName;
}

Reflected Reflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>::operator()(
    const TypedLogicalOperator<StatisticStoreWriterLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedStatisticStoreWriterLogicalOperator{
        .operatorId = op.getId(), .statisticId = op->getStatisticId(), .typeName = op->getTypeName()});
}

Unreflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<StatisticStoreWriterLogicalOperator>
Unreflector<TypedLogicalOperator<StatisticStoreWriterLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, statisticId, typeName] = context.unreflect<detail::ReflectedStatisticStoreWriterLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("StatisticStoreWriterLogicalOperator must have exactly one child, but got {}", children.size());
    }
    return StatisticStoreWriterLogicalOperator::create(children.at(0), statisticId, std::move(typeName));
}

}

std::size_t std::hash<NES::StatisticStoreWriterLogicalOperator>::operator()(
    const NES::StatisticStoreWriterLogicalOperator& statisticStoreWriterLogicalOperator) const noexcept
{
    return folly::hash::hash_combine_generic(
        NES::Hash{}, statisticStoreWriterLogicalOperator.statisticId.getRawValue(), statisticStoreWriterLogicalOperator.typeName);
}
