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

#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

/// NOLINTNEXTLINE(misc-include-cleaner)
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/Reprojecter.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Serialization/WindowAggregationLogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/Hash.hpp> /// NOLINT(misc-include-cleaner)
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Variant.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    WeakLogicalOperator self,
    GroupingKeyType groupingKey,
    std::vector<ProjectedAggregation> aggregationFunctions,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
    : ManagedByOperator(std::move(self))
    , windowType(std::move(windowType))
    , groupingKeys(std::move(groupingKey))
    , aggregationFunctions(std::move(aggregationFunctions))
    , timestampField(std::move(timeCharacteristic))
{
}

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    WeakLogicalOperator self,
    LogicalOperator child,
    GroupingKeyType groupingKey,
    std::vector<ProjectedAggregation> aggregationFunctions,
    Windowing::TimeBasedWindowType windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , windowType(std::move(windowType))
    , groupingKeys(std::move(groupingKey))
    , aggregationFunctions(std::move(aggregationFunctions))
    , timestampField(std::move(timeCharacteristic))
{
    inferLocalSchema();
}

bool operator==(
    const WindowedAggregationLogicalOperator::ProjectedAggregation& lhs,
    const WindowedAggregationLogicalOperator::ProjectedAggregation& rhs)
{
    return lhs.function == rhs.function && lhs.name == rhs.name;
}

void WindowedAggregationLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const Schema<Field, Unordered>& inputSchema = child->getOutputSchema();

    groupingKeys = std::visit(
        [&inputSchema](const auto& visitingGroupingKey) -> GroupingKeyType
        {
            using VectorType = std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>;
            return visitingGroupingKey
                | std::views::transform(
                       [&](const auto& key) -> typename VectorType::value_type
                       {
                           return std::make_pair(
                               key.first.withInferredDataType(inputSchema).template getAs<FieldAccessLogicalFunction>(), key.second);
                       })
                | std::ranges::to<VectorType>();
        },
        groupingKeys);

    std::vector<ProjectedAggregation> newFunctions;
    for (const auto& [function, identifier] : aggregationFunctions)
    {
        auto inferredFunction = function->withInferredType(inputSchema);
        newFunctions.emplace_back(inferredFunction, identifier);
    }
    aggregationFunctions = newFunctions;

    timestampField = Windowing::TimeCharacteristicWrapper{std::move(timestampField)}.withInferredSchema(inputSchema);

    std::vector<UnqualifiedUnboundField> outputFields{};

    outputFields.push_back(startEndFields[0]);
    outputFields.push_back(startEndFields[1]);

    for (const auto& [aggFunction, name] : aggregationFunctions)
    {
        outputFields.emplace_back(name, aggFunction->getAggregateType());
    }
    const auto fieldsAreBound
        = std::holds_alternative<std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>>(
            groupingKeys);
    PRECONDITION(fieldsAreBound, "Internal schema inference of windowed aggregation called before binding grouping keys");
    for (const auto& [aggFunction, name] :
         std::get<std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>>(groupingKeys))
    {
        if (name.has_value())
        {
            outputFields.emplace_back(name.value(), aggFunction.getDataType());
        }
    }

    const auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (not outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in inpu schema: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }

    outputSchema = outputSchemaOrCollisions.value();
}

std::string_view WindowedAggregationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string WindowedAggregationLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    const std::variant<std::vector<std::string>, std::vector<uint64_t>> myVariant{std::vector<std::string>{}};
    if (verbosity == ExplainVerbosity::Debug)
    {
        auto windowType = getWindowType();
        auto windowAggregation = getWindowAggregation();
        return fmt::format(
            "WINDOW AGGREGATION(opId: {}, {}, window type: {})",
            id,
            fmt::join(
                std::views::transform(
                    windowAggregation,
                    [&](const auto& agg) { return fmt::format("{} as {}", agg.name, agg.function->explain(verbosity)); }),
                ", "),
            windowType);
    }
    auto windowAggregation = getWindowAggregation();
    return fmt::format(
        "WINDOW AGG({})",
        fmt::join(
            std::views::transform(
                windowAggregation, [&](const auto& agg) { return fmt::format("{} as {}", agg.name, agg.function->explain(verbosity)); }),
            ", "));
}

bool WindowedAggregationLogicalOperator::operator==(const WindowedAggregationLogicalOperator& rhs) const
{
    if (this->isKeyed() != rhs.isKeyed())
    {
        return false;
    }

    if (!std::visit(
            [](const auto& thisGroupingKey, const auto& rhsGroupingKeys)
            {
                if (thisGroupingKey.size() != rhsGroupingKeys.size())
                {
                    return false;
                }

                for (uint64_t i = 0; i < thisGroupingKey.size(); i++)
                {
                    if (thisGroupingKey[i].second != rhsGroupingKeys[i].second
                        || !thisGroupingKey[i].first.operator==(rhsGroupingKeys[i].first))
                    {
                        return false;
                    }
                }
                return true;
            },
            groupingKeys,
            rhs.groupingKeys))
    {
        return false;
    }

    const auto rhsWindowAggregation = rhs.getWindowAggregation();
    if (aggregationFunctions.size() != rhsWindowAggregation.size())
    {
        return false;
    }
    for (uint64_t i = 0; i < aggregationFunctions.size(); i++)
    {
        if ((aggregationFunctions[i]) != (rhsWindowAggregation[i]))
        {
            return false;
        }
    }

    return windowType == rhs.windowType && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

WindowedAggregationLogicalOperator WindowedAggregationLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Ordered> WindowedAggregationLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider) const
{
    auto boundStartEndFields = startEndFields | RangeBinder{self.lock()} | std::ranges::to<Schema<Field, Ordered>>();
    auto keyFields
        = std::get<std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>>(groupingKeys)
        | std::views::filter([](const auto& key) { return key.second.has_value(); })
        | std::views::transform([this](const auto& key) { return bind(self.lock(), key.second.value()); })
        | std::ranges::to<Schema<Field, Ordered>>();
    auto aggOutput = aggregationFunctions | std::views::transform([](const auto& agg) { return agg.name; }) | RangeBinder{self.lock()}
        | std::ranges::to<Schema<Field, Ordered>>();

    return std::array{std::move(boundStartEndFields), std::move(keyFields), std::move(aggOutput)} | std::views::join
        | std::ranges::to<Schema<Field, Ordered>>();
}

const detail::DynamicBase* WindowedAggregationLogicalOperator::getDynamicBase() const
{
    return static_cast<const Reprojecter*>(this);
}

TraitSet WindowedAggregationLogicalOperator::getTraitSet() const
{
    return traitSet;
}

WindowedAggregationLogicalOperator WindowedAggregationLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

WindowedAggregationLogicalOperator WindowedAggregationLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for windowedAggregation, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

Schema<Field, Unordered> WindowedAggregationLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> WindowedAggregationLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator WindowedAggregationLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !std::visit([](const auto& keys) { return keys.empty(); }, groupingKeys);
}

std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return aggregationFunctions;
}

Windowing::TimeBasedWindowType WindowedAggregationLogicalOperator::getWindowType() const
{
    return windowType;
}

VariantContainer<std::vector, TypedLogicalFunction<UnboundFieldAccessLogicalFunction>, TypedLogicalFunction<FieldAccessLogicalFunction>>
WindowedAggregationLogicalOperator::getGroupingKeys() const
{
    return std::visit(
        [](const auto& keys)
        {
            return VariantContainer<
                std::vector,
                TypedLogicalFunction<UnboundFieldAccessLogicalFunction>,
                TypedLogicalFunction<FieldAccessLogicalFunction>>{
                keys | std::views::transform([](const auto& key) { return key.first; }) | std::ranges::to<std::vector>()};
        },
        groupingKeys);
}

std::unordered_map<Field, std::unordered_set<Field>> WindowedAggregationLogicalOperator::getAccessedFieldsForOutput() const
{
    const auto isBound
        = std::holds_alternative<std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>>(
            groupingKeys);
    INVARIANT(isBound, "Tried accessing accessed fields for output before calling schema inference");
    auto boundGroupingKeys
        = std::get<std::vector<std::pair<TypedLogicalFunction<FieldAccessLogicalFunction>, std::optional<Identifier>>>>(groupingKeys);

    const auto boundOutputSchema = getOutputSchema();

    std::unordered_map<Field, std::unordered_set<Field>> accessedFields;
    for (const auto& [aggFunction, name] : getWindowAggregation())
    {
        const auto writeFieldOpt = boundOutputSchema.getFieldByName(name);
        INVARIANT(writeFieldOpt.has_value(), "Field {} not found in output schema", name);
        const bool fieldAccessInferred
            = std::holds_alternative<TypedLogicalFunction<FieldAccessLogicalFunction>>(aggFunction->getInputFunction());
        INVARIANT(
            fieldAccessInferred,
            "Field access not inferred for aggregation function but schema inference was called {}",
            aggFunction->getName());
        auto fieldAccessFunction = std::get<TypedLogicalFunction<FieldAccessLogicalFunction>>(aggFunction->getInputFunction());
        accessedFields[writeFieldOpt.value()].insert(fieldAccessFunction->getField());
    }
    return accessedFields;
}

const WindowedAggregationLogicalOperator::GroupingKeyType& WindowedAggregationLogicalOperator::getGroupingKeysWithName() const
{
    return groupingKeys;
}

const UnqualifiedUnboundField& WindowedAggregationLogicalOperator::getWindowStartField() const
{
    return startEndFields[0];
}

const UnqualifiedUnboundField& WindowedAggregationLogicalOperator::getWindowEndField() const
{
    return startEndFields[1];
}

std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>
WindowedAggregationLogicalOperator::getCharacteristic() const
{
    return timestampField;
}

Reflected Reflector<TypedLogicalOperator<WindowedAggregationLogicalOperator>>::operator()(
    const TypedLogicalOperator<WindowedAggregationLogicalOperator>& op) const
{
    return reflect(detail::ReflectedWindowAggregationLogicalOperator{
        .operatorId = op.getId(),
        .windowType = op->getWindowType(),
        .groupingKeys = op->getGroupingKeysWithName(),
        .aggregations = op->getWindowAggregation(),
        .timestampField = op->getCharacteristic()});
}

Unreflector<TypedLogicalOperator<WindowedAggregationLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<WindowedAggregationLogicalOperator> Unreflector<TypedLogicalOperator<WindowedAggregationLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, windowType, keys, aggregations, timestampField]
        = context.unreflect<detail::ReflectedWindowAggregationLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("WindowedAggregationLogicalOperator must have exactly one child, but got {}", children.size());
    }
    return TypedLogicalOperator<WindowedAggregationLogicalOperator>{children.at(0), keys, aggregations, windowType, timestampField};
}

}

std::size_t std::hash<NES::WindowedAggregationLogicalOperator>::operator()(
    const NES::WindowedAggregationLogicalOperator& windowedAggregationLogicalOperator) const noexcept
{
    return folly::hash::hash_combine(
        windowedAggregationLogicalOperator.windowType,
        windowedAggregationLogicalOperator.timestampField,
        windowedAggregationLogicalOperator.groupingKeys,
        windowedAggregationLogicalOperator.aggregationFunctions);
}

std::size_t std::hash<NES::WindowedAggregationLogicalOperator::ProjectedAggregation>::operator()(
    const NES::WindowedAggregationLogicalOperator::ProjectedAggregation& aggregation) const noexcept
{
    return folly::hash::hash_combine(aggregation.function, aggregation.name);
}
