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

#include <Operators/StreamTableJoinLogicalOperator.hpp>

#include <array>
#include <functional>
#include <ranges>
#include <utility>

#include <fmt/format.h>
#include <folly/hash/Hash.h>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/Hash.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{
namespace
{
std::string_view joinTypeName(const StreamTableJoinLogicalOperator::JoinType joinType)
{
    switch (joinType)
    {
        case StreamTableJoinLogicalOperator::JoinType::INNER_JOIN:
            return "INNER";
        case StreamTableJoinLogicalOperator::JoinType::LEFT_SEMI_JOIN:
            return "LEFT_SEMI";
    }
    std::unreachable();
}
}

StreamTableJoinLogicalOperator::StreamTableJoinLogicalOperator(
    WeakLogicalOperator self,
    LogicalFunction joinFunction,
    std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics,
    const JoinType joinType)
    : ManagedByOperator(std::move(self))
    , joinFunction(std::move(joinFunction))
    , timeCharacteristics(std::move(timeCharacteristics))
    , joinType(joinType)
{
}

StreamTableJoinLogicalOperator::StreamTableJoinLogicalOperator(
    WeakLogicalOperator self,
    std::array<LogicalOperator, 2> children,
    LogicalFunction joinFunction,
    std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics,
    const JoinType joinType)
    : ManagedByOperator(std::move(self))
    , children(std::move(children))
    , joinFunction(std::move(joinFunction))
    , timeCharacteristics(std::move(timeCharacteristics))
    , joinType(joinType)
{
    inferLocalSchema();
}

TypedLogicalOperator<StreamTableJoinLogicalOperator> StreamTableJoinLogicalOperator::create(
    LogicalFunction joinFunction, std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics, const JoinType joinType)
{
    return TypedLogicalOperator<StreamTableJoinLogicalOperator>{std::move(joinFunction), std::move(timeCharacteristics), joinType};
}

TypedLogicalOperator<StreamTableJoinLogicalOperator> StreamTableJoinLogicalOperator::create(
    std::array<LogicalOperator, 2> children,
    LogicalFunction joinFunction,
    std::optional<StreamTableJoinTimeCharacteristics> timeCharacteristics,
    const JoinType joinType)
{
    return TypedLogicalOperator<StreamTableJoinLogicalOperator>{
        std::move(children), std::move(joinFunction), std::move(timeCharacteristics), joinType};
}

void StreamTableJoinLogicalOperator::inferLocalSchema()
{
    PRECONDITION(children.has_value(), "Children not set when inferring stream-table join schema");

    const auto inputFields = *children | std::views::transform([](const auto& child) { return child.getOutputSchema(); }) | std::views::join
        | std::ranges::to<std::vector>();
    auto inputSchema = Schema<Field, Unordered>::tryCreateCollisionFree(inputFields);
    if (!inputSchema.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in stream-table join input schemas: " + Schema<Field, Unordered>::createCollisionString(inputSchema.error()));
    }

    joinFunction = joinFunction.withInferredDataType(inputSchema.value());

    if (timeCharacteristics.has_value())
    {
        timeCharacteristics = std::visit(
            [&](const auto& characteristics) -> StreamTableJoinTimeCharacteristics
            {
                return std::array{
                    Windowing::TimeCharacteristicWrapper{characteristics[0]}.withInferredSchema(inputSchema.value()),
                    Windowing::TimeCharacteristicWrapper{characteristics[1]}.withInferredSchema(inputSchema.value())};
            },
            timeCharacteristics.value());
    }

    const auto semiJoinOutputFields = children.value()[0].getOutputSchema()
        | std::views::transform([](const Field& field) { return field.unbound(); }) | std::ranges::to<std::vector>();
    const auto innerJoinOutputFields
        = inputFields | std::views::transform([](const Field& field) { return field.unbound(); }) | std::ranges::to<std::vector>();
    const auto& outputFields = joinType == JoinType::LEFT_SEMI_JOIN ? semiJoinOutputFields : innerJoinOutputFields;
    auto inferredOutputSchema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    INVARIANT(inferredOutputSchema.has_value(), "Input schema was collision free but unbound output schema was not");
    outputSchema = std::move(inferredOutputSchema.value());
}

LogicalFunction StreamTableJoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

StreamTableJoinLogicalOperator::JoinType StreamTableJoinLogicalOperator::getJoinType() const
{
    return joinType;
}

std::optional<StreamTableJoinTimeCharacteristics> StreamTableJoinLogicalOperator::getTimeCharacteristics() const
{
    return timeCharacteristics;
}

std::array<LogicalOperator, 2> StreamTableJoinLogicalOperator::getBothChildren() const
{
    PRECONDITION(children.has_value(), "Children not set when accessing stream-table join children");
    return children.value();
}

bool StreamTableJoinLogicalOperator::operator==(const StreamTableJoinLogicalOperator& rhs) const
{
    return joinFunction == rhs.joinFunction && timeCharacteristics == rhs.timeCharacteristics && joinType == rhs.joinType
        && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

StreamTableJoinLogicalOperator StreamTableJoinLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

TraitSet StreamTableJoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

Schema<Field, Unordered> StreamTableJoinLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed stream-table join schema before inference");
    return bindToOperator(self.lock(), outputSchema.value());
}

StreamTableJoinLogicalOperator StreamTableJoinLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "Stream-table join requires exactly two children");
    auto copy = *this;
    copy.children = std::array{std::move(newChildren[0]), std::move(newChildren[1])};
    return copy;
}

StreamTableJoinLogicalOperator StreamTableJoinLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = withChildrenUnsafe(std::move(newChildren));
    copy.inferLocalSchema();
    return copy;
}

std::vector<LogicalOperator> StreamTableJoinLogicalOperator::getChildren() const
{
    if (!children.has_value())
    {
        return {};
    }
    return children.value() | std::ranges::to<std::vector>();
}

std::string StreamTableJoinLogicalOperator::explain(const ExplainVerbosity verbosity, const OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "StreamTableJoin(opId: {}, type: {}, joinFunction: {}, timed: {}, traitSet: {})",
            id,
            joinTypeName(joinType),
            joinFunction.explain(verbosity),
            timeCharacteristics.has_value(),
            traitSet.explain(verbosity));
    }
    return fmt::format("StreamTableJoin({}, {})", joinTypeName(joinType), joinFunction.explain(verbosity));
}

std::string_view StreamTableJoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

StreamTableJoinLogicalOperator StreamTableJoinLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.has_value(), "Children not set when inferring stream-table join schema");
    auto copy = *this;
    copy.children = std::array{children.value()[0].withInferredSchema(), children.value()[1].withInferredSchema()};
    copy.inferLocalSchema();
    return copy;
}

Reflected Reflector<TypedLogicalOperator<StreamTableJoinLogicalOperator>>::operator()(
    const TypedLogicalOperator<StreamTableJoinLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedStreamTableJoinLogicalOperator{
        .operatorId = op.getId(),
        .joinFunction = op->getJoinFunction(),
        .timeCharacteristics = op->getTimeCharacteristics(),
        .joinType = op->getJoinType()});
}

Unreflector<TypedLogicalOperator<StreamTableJoinLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<StreamTableJoinLogicalOperator> Unreflector<TypedLogicalOperator<StreamTableJoinLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    const auto data = context.unreflect<detail::ReflectedStreamTableJoinLogicalOperator>(reflected);
    const auto children = plan->getChildrenFor(data.operatorId, context);
    PRECONDITION(children.size() == 2, "Reflected stream-table join requires exactly two children");
    return StreamTableJoinLogicalOperator::create(
        std::array{children[0], children[1]}, data.joinFunction, data.timeCharacteristics, data.joinType);
}

}

std::size_t std::hash<NES::StreamTableJoinLogicalOperator>::operator()(const NES::StreamTableJoinLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.joinFunction, op.timeCharacteristics.has_value(), op.joinType);
}
