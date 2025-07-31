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

#include <Operators/Windows/JoinLogicalOperator.hpp>

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

/// NOLINTNEXTLINE(misc-include-cleaner)
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Hash.hpp> /// NOLINT(misc-include-cleaner)
#include <Util/Overloaded.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{


std::optional<JoinTimeCharacteristic> JoinLogicalOperator::createJoinTimeCharacteristic(
    const std::array<std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>, 2>& timestampFields)
{
    return std::visit(
        Overloaded{
            [&timestampFields](const Windowing::UnboundTimeCharacteristic& unbound)
            {
                return std::visit(
                    Overloaded{
                        [&unbound](const Windowing::UnboundTimeCharacteristic& unbound2) -> std::optional<JoinTimeCharacteristic>
                        { return std::make_optional<JoinTimeCharacteristic>(std::array{unbound, unbound2}); },
                        [](const Windowing::BoundTimeCharacteristic&) -> std::optional<JoinTimeCharacteristic>
                        { return std::optional<JoinTimeCharacteristic>{}; }},
                    timestampFields[1]);
            },
            [&timestampFields](Windowing::BoundTimeCharacteristic bound)
            {
                return std::visit(
                    Overloaded{
                        [&bound](const Windowing::BoundTimeCharacteristic& bound2) -> std::optional<JoinTimeCharacteristic>
                        { return std::make_optional<JoinTimeCharacteristic>(std::array{bound, bound2}); },
                        [](const Windowing::UnboundTimeCharacteristic&) -> std::optional<JoinTimeCharacteristic>
                        { return std::optional<JoinTimeCharacteristic>{}; }},
                    timestampFields[1]);
            }},
        timestampFields[0]);
}

JoinLogicalOperator::JoinLogicalOperator(
    WeakLogicalOperator self,
    LogicalFunction joinFunction,
    Windowing::TimeBasedWindowType windowType,
    JoinType joinType,
    JoinTimeCharacteristic timeCharacteristics)
    : self(std::move(self))
    , windowType(std::move(windowType))
    , joinType(joinType)
    , joinFunction(std::move(joinFunction))
    , timestampFields(std::move(timeCharacteristics))
{
}

JoinLogicalOperator::JoinLogicalOperator(
    WeakLogicalOperator self,
    std::array<LogicalOperator, 2> children,
    LogicalFunction joinFunction,
    Windowing::TimeBasedWindowType windowType,
    const JoinType joinType,
    JoinTimeCharacteristic timeCharacteristics)
    : self(std::move(self))
    , windowType(std::move(windowType))
    , joinType(joinType)
    , children(std::move(children))
    , joinFunction(std::move(joinFunction))
    , timestampFields(std::move(timeCharacteristics))
{
    inferLocalSchema();
}

std::string_view JoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool JoinLogicalOperator::operator==(const JoinLogicalOperator& rhs) const
{
    return getWindowType() == rhs.getWindowType() and getJoinFunction() == rhs.getJoinFunction() and outputSchema == rhs.outputSchema
        and getTraitSet() == rhs.getTraitSet();
}

std::string JoinLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "Join(opId: {}, windowType: {}, joinFunction: {}, windowMetadata: (startField: {}, endField: {}), traitSet: {})",
            id,
            getWindowType(),
            getJoinFunction().explain(verbosity),
            startEndFields.at(0),
            startEndFields.at(1),
            traitSet.explain(verbosity));
    }
    return fmt::format("Join({})", getJoinFunction().explain(verbosity));
}

JoinLogicalOperator::JoinType JoinLogicalOperator::getJoinType() const
{
    return joinType;
}

void JoinLogicalOperator::inferLocalSchema()
{
    PRECONDITION(children.has_value(), "Child not set when calling schema inference");
    const std::vector<Field> inputFields = *children | std::views::transform([](const auto& child) { return child.getOutputSchema(); })
        | std::views::join | std::ranges::to<std::vector>();

    auto inputSchemaOrCollisions = Schema<Field, Unordered>::tryCreateCollisionFree(inputFields);

    if (!inputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in input schemas: " + Schema<Field, Unordered>::createCollisionString(inputSchemaOrCollisions.error()));
    }
    const auto& inputSchema = inputSchemaOrCollisions.value();
    this->timestampFields = std::visit(
        [&](const auto& tsFields)
        {
            return std::array{
                Windowing::TimeCharacteristicWrapper{tsFields[0]}.withInferredSchema(inputSchema),
                Windowing::TimeCharacteristicWrapper{tsFields[1]}.withInferredSchema(inputSchema)};
        },
        this->timestampFields);

    this->joinFunction = this->joinFunction.withInferredDataType(inputSchemaOrCollisions.value());

    std::vector<UnqualifiedUnboundField> outputFields
        = inputFields | std::views::transform([](const Field& field) { return field.unbound(); }) | std::ranges::to<std::vector>();

    outputFields.emplace_back(startEndFields[0].getFullyQualifiedName(), startEndFields[0].getDataType());
    outputFields.emplace_back(startEndFields[1].getFullyQualifiedName(), startEndFields[1].getDataType());

    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);

    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in input schemas with added fields from windows join: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }

    this->outputSchema = outputSchemaOrCollisions.value();
}

JoinLogicalOperator JoinLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.children = {(*copy.children)[0].withInferredSchema(), (*copy.children)[1].withInferredSchema()};
    copy.inferLocalSchema();
    return copy;
}

JoinLogicalOperator JoinLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet JoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

JoinLogicalOperator JoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 2, "Can only set exactly two child for join, got {}", children.size());
    auto copy = *this;
    copy.children = std::array{std::move(children.at(0)), std::move(children.at(1))};
    return copy;
}

Schema<Field, Unordered> JoinLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> JoinLogicalOperator::getChildren() const
{
    if (children.has_value())
    {
        return *children | std::ranges::to<std::vector>();
    }
    return {};
}

std::array<LogicalOperator, 2> JoinLogicalOperator::getBothChildren() const
{
    PRECONDITION(children.has_value(), "Children not set when trying to retrieve join children");
    return *children;
}

Windowing::TimeBasedWindowType JoinLogicalOperator::getWindowType() const
{
    return windowType;
}

const UnqualifiedUnboundField& JoinLogicalOperator::getStartField() const
{
    return startEndFields[0];
}

const UnqualifiedUnboundField& JoinLogicalOperator::getEndField() const
{
    return startEndFields[1];
}

LogicalFunction JoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

JoinTimeCharacteristic JoinLogicalOperator::getJoinTimeCharacteristics() const
{
    return timestampFields;
}

Reflected Reflector<TypedLogicalOperator<JoinLogicalOperator>>::operator()(const TypedLogicalOperator<JoinLogicalOperator>& op) const
{
    return reflect(detail::ReflectedJoinLogicalOperator{
        .operatorId = op.getId(),
        .joinFunction = op->getJoinFunction(),
        .windowType = op->getWindowType(),
        .timestampFields = op->getJoinTimeCharacteristics(),
        .joinType = op->getJoinType()});
}

Unreflector<TypedLogicalOperator<JoinLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<JoinLogicalOperator>
Unreflector<TypedLogicalOperator<JoinLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [operatorId, joinFunction, windowType, timestampFields, joinType]
        = context.unreflect<detail::ReflectedJoinLogicalOperator>(reflected);
    auto foundChildren = plan->getChildrenFor(operatorId, context);
    return TypedLogicalOperator<JoinLogicalOperator>{
        std::array{foundChildren.at(0), foundChildren.at(1)}, std::move(joinFunction), windowType, joinType, std::move(timestampFields)};
}

}

std::size_t std::hash<NES::JoinLogicalOperator>::operator()(const NES::JoinLogicalOperator& joinLogicalOperator) const noexcept
{
    return folly::hash::hash_combine(
        joinLogicalOperator.joinType,
        joinLogicalOperator.joinFunction,
        joinLogicalOperator.windowType,
        joinLogicalOperator.timestampFields,
        joinLogicalOperator.startEndFields);
}
