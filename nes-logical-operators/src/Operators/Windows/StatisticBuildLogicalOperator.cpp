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

#include <Operators/Windows/StatisticBuildLogicalOperator.hpp>

#include <cstddef>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Statistic/LogicalStatisticFields.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Serialization/WindowAggregationLogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

StatisticBuildLogicalOperator::StatisticBuildLogicalOperator(
    WeakLogicalOperator self,
    std::vector<StatisticAggregation> statisticAggregations,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic,
    LogicalStatisticFields logicalStatisticFields)
    : ManagedByOperator(std::move(self))
    , windowType(std::move(windowType))
    , statisticAggregations(std::move(statisticAggregations))
    , timestampField(std::move(timeCharacteristic))
    , logicalStatisticFields(std::move(logicalStatisticFields))
{
}

StatisticBuildLogicalOperator::StatisticBuildLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    std::vector<StatisticAggregation> statisticAggregations,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic,
    LogicalStatisticFields logicalStatisticFields)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , windowType(std::move(windowType))
    , statisticAggregations(std::move(statisticAggregations))
    , timestampField(std::move(timeCharacteristic))
    , logicalStatisticFields(std::move(logicalStatisticFields))
{
    inferLocalSchema();
}

TypedLogicalOperator<StatisticBuildLogicalOperator> StatisticBuildLogicalOperator::create(
    std::vector<StatisticAggregation> statisticAggregations,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic,
    LogicalStatisticFields logicalStatisticFields)
{
    return TypedLogicalOperator<StatisticBuildLogicalOperator>{
        std::move(statisticAggregations), std::move(windowType), std::move(timeCharacteristic), std::move(logicalStatisticFields)};
}

TypedLogicalOperator<StatisticBuildLogicalOperator> StatisticBuildLogicalOperator::create(
    LogicalOperator child,
    std::vector<StatisticAggregation> statisticAggregations,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic,
    LogicalStatisticFields logicalStatisticFields)
{
    return TypedLogicalOperator<StatisticBuildLogicalOperator>{
        std::move(child),
        std::move(statisticAggregations),
        std::move(windowType),
        std::move(timeCharacteristic),
        std::move(logicalStatisticFields)};
}

void StatisticBuildLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const Schema<Field, Unordered>& inputSchema = child->getOutputSchema();

    for (auto& [function, statisticId] : statisticAggregations)
    {
        function = function->withInferredType(inputSchema);
    }

    timestampField = Windowing::TimeCharacteristicWrapper{std::move(timestampField)}.withInferredSchema(inputSchema);

    std::vector<UnqualifiedUnboundField> outputFields{};

    outputFields.push_back(logicalStatisticFields.statisticStartTsField);
    outputFields.push_back(logicalStatisticFields.statisticEndTsField);

    if (statisticAggregations.empty())
    {
        throw CannotInferSchema("A StatisticBuild operator requires at least one statistic aggregation");
    }
    const auto dataFieldType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    for (const auto& [function, statisticId] : statisticAggregations)
    {
        outputFields.emplace_back(Identifier::parse(statisticDataFieldName(statisticId)), dataFieldType);
    }

    outputFields.push_back(logicalStatisticFields.statisticNumberOfSeenMeasurementsField);

    const auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (not outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in output schema: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }

    outputSchema = outputSchemaOrCollisions.value();
}

std::string_view StatisticBuildLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string StatisticBuildLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "STATISTIC BUILD(opId: {}, {}, window type: {})",
            id,
            fmt::join(
                std::views::transform(statisticAggregations, [&](const auto& agg) { return agg.function->explain(verbosity); }), ", "),
            windowType);
    }
    return fmt::format(
        "STAT BUILD({})",
        fmt::join(std::views::transform(statisticAggregations, [&](const auto& agg) { return agg.function->explain(verbosity); }), ", "));
}

bool StatisticBuildLogicalOperator::operator==(const StatisticBuildLogicalOperator& rhs) const
{
    return statisticAggregations == rhs.statisticAggregations && windowType == rhs.windowType && outputSchema == rhs.outputSchema
        && traitSet == rhs.traitSet && timestampField == rhs.timestampField && logicalStatisticFields == rhs.logicalStatisticFields;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet StatisticBuildLogicalOperator::getTraitSet() const
{
    return traitSet;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticBuild, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

StatisticBuildLogicalOperator StatisticBuildLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for StatisticBuild, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> StatisticBuildLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> StatisticBuildLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator StatisticBuildLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

const std::vector<StatisticAggregation>& StatisticBuildLogicalOperator::getStatisticAggregations() const
{
    return statisticAggregations;
}

std::vector<WindowAggregationLogicalFunction> StatisticBuildLogicalOperator::getWindowAggregation() const
{
    std::vector<WindowAggregationLogicalFunction> functions;
    functions.reserve(statisticAggregations.size());
    for (const auto& agg : statisticAggregations)
    {
        functions.push_back(agg.function);
    }
    return functions;
}

Windowing::TimeBasedWindowType StatisticBuildLogicalOperator::getWindowType() const
{
    return windowType;
}

const LogicalStatisticFields& StatisticBuildLogicalOperator::getLogicalStatisticFields() const
{
    return logicalStatisticFields;
}

const UnqualifiedUnboundField& StatisticBuildLogicalOperator::getWindowStartField() const
{
    return logicalStatisticFields.statisticStartTsField;
}

const UnqualifiedUnboundField& StatisticBuildLogicalOperator::getWindowEndField() const
{
    return logicalStatisticFields.statisticEndTsField;
}

const UnqualifiedUnboundField& StatisticBuildLogicalOperator::getNumberOfSeenMeasurementsField() const
{
    return logicalStatisticFields.statisticNumberOfSeenMeasurementsField;
}

std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>
StatisticBuildLogicalOperator::getCharacteristic() const
{
    return timestampField;
}

Reflected Reflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>::operator()(
    const TypedLogicalOperator<StatisticBuildLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedStatisticBuildLogicalOperator{
        .operatorId = op.getId(),
        .windowType = op->getWindowType(),
        .statisticAggregations = op->getStatisticAggregations(),
        .timestampField = op->getCharacteristic()});
}

Unreflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<StatisticBuildLogicalOperator> Unreflector<TypedLogicalOperator<StatisticBuildLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, windowType, statisticAggregations, timestampField]
        = context.unreflect<detail::ReflectedStatisticBuildLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("StatisticBuildLogicalOperator must have exactly one child, but got {}", children.size());
    }
    return StatisticBuildLogicalOperator::create(
        children.at(0), std::move(statisticAggregations), std::move(windowType), std::move(timestampField));
}

}

std::size_t std::hash<NES::StatisticBuildLogicalOperator>::operator()(const NES::StatisticBuildLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.windowType, op.timestampField, op.statisticAggregations);
}
