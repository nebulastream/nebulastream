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

#include <Operators/AsOfJoinLogicalOperator.hpp>

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

AsOfJoinLogicalOperator::AsOfJoinLogicalOperator(
    WeakLogicalOperator self, LogicalFunction joinFunction, AsOfJoinTimeCharacteristics timeCharacteristics, const bool rightIsTable)
    : ManagedByOperator(std::move(self))
    , joinFunction(std::move(joinFunction))
    , timeCharacteristics(std::move(timeCharacteristics))
    , rightIsTable(rightIsTable)
{
}

AsOfJoinLogicalOperator::AsOfJoinLogicalOperator(
    WeakLogicalOperator self,
    std::array<LogicalOperator, 2> children,
    LogicalFunction joinFunction,
    AsOfJoinTimeCharacteristics timeCharacteristics,
    const bool rightIsTable)
    : ManagedByOperator(std::move(self))
    , children(std::move(children))
    , joinFunction(std::move(joinFunction))
    , timeCharacteristics(std::move(timeCharacteristics))
    , rightIsTable(rightIsTable)
{
    inferLocalSchema();
}

TypedLogicalOperator<AsOfJoinLogicalOperator>
AsOfJoinLogicalOperator::create(LogicalFunction joinFunction, AsOfJoinTimeCharacteristics timeCharacteristics, const bool rightIsTable)
{
    return TypedLogicalOperator<AsOfJoinLogicalOperator>{std::move(joinFunction), std::move(timeCharacteristics), rightIsTable};
}

TypedLogicalOperator<AsOfJoinLogicalOperator> AsOfJoinLogicalOperator::create(
    std::array<LogicalOperator, 2> children,
    LogicalFunction joinFunction,
    AsOfJoinTimeCharacteristics timeCharacteristics,
    const bool rightIsTable)
{
    return TypedLogicalOperator<AsOfJoinLogicalOperator>{
        std::move(children), std::move(joinFunction), std::move(timeCharacteristics), rightIsTable};
}

void AsOfJoinLogicalOperator::inferLocalSchema()
{
    PRECONDITION(children.has_value(), "Children not set when inferring ASOF join schema");

    const auto inputFields = *children | std::views::transform([](const auto& child) { return child.getOutputSchema(); }) | std::views::join
        | std::ranges::to<std::vector>();
    auto inputSchema = Schema<Field, Unordered>::tryCreateCollisionFree(inputFields);
    if (!inputSchema.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in ASOF join input schemas: " + Schema<Field, Unordered>::createCollisionString(inputSchema.error()));
    }

    joinFunction = joinFunction.withInferredDataType(inputSchema.value());
    timeCharacteristics = std::visit(
        [&](const auto& characteristics) -> AsOfJoinTimeCharacteristics
        {
            return std::array{
                Windowing::TimeCharacteristicWrapper{characteristics[0]}.withInferredSchema(inputSchema.value()),
                Windowing::TimeCharacteristicWrapper{characteristics[1]}.withInferredSchema(inputSchema.value())};
        },
        timeCharacteristics);

    const auto outputFields
        = inputFields | std::views::transform([](const Field& field) { return field.unbound(); }) | std::ranges::to<std::vector>();
    auto inferredOutputSchema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    INVARIANT(inferredOutputSchema.has_value(), "Input schema was collision free but unbound output schema was not");
    outputSchema = std::move(inferredOutputSchema.value());
}

LogicalFunction AsOfJoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

AsOfJoinTimeCharacteristics AsOfJoinLogicalOperator::getTimeCharacteristics() const
{
    return timeCharacteristics;
}

bool AsOfJoinLogicalOperator::isRightTable() const
{
    return rightIsTable;
}

std::array<LogicalOperator, 2> AsOfJoinLogicalOperator::getBothChildren() const
{
    PRECONDITION(children.has_value(), "Children not set when accessing ASOF join children");
    return children.value();
}

bool AsOfJoinLogicalOperator::operator==(const AsOfJoinLogicalOperator& rhs) const
{
    return joinFunction == rhs.joinFunction && timeCharacteristics == rhs.timeCharacteristics && rightIsTable == rhs.rightIsTable
        && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

AsOfJoinLogicalOperator AsOfJoinLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

TraitSet AsOfJoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

Schema<Field, Unordered> AsOfJoinLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed ASOF join schema before inference");
    return bindToOperator(self.lock(), outputSchema.value());
}

AsOfJoinLogicalOperator AsOfJoinLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "ASOF join requires exactly two children");
    auto copy = *this;
    copy.children = std::array{std::move(newChildren[0]), std::move(newChildren[1])};
    return copy;
}

AsOfJoinLogicalOperator AsOfJoinLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = withChildrenUnsafe(std::move(newChildren));
    copy.inferLocalSchema();
    return copy;
}

std::vector<LogicalOperator> AsOfJoinLogicalOperator::getChildren() const
{
    if (!children.has_value())
    {
        return {};
    }
    return children.value() | std::ranges::to<std::vector>();
}

std::string AsOfJoinLogicalOperator::explain(const ExplainVerbosity verbosity, const OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "AsOfJoin(opId: {}, rightIsTable: {}, joinFunction: {}, traitSet: {})",
            id,
            rightIsTable,
            joinFunction.explain(verbosity),
            traitSet.explain(verbosity));
    }
    return fmt::format("AsOfJoin({}, {})", rightIsTable ? "TABLE" : "STREAM", joinFunction.explain(verbosity));
}

std::string_view AsOfJoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

AsOfJoinLogicalOperator AsOfJoinLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.has_value(), "Children not set when inferring ASOF join schema");
    auto copy = *this;
    copy.children = std::array{children.value()[0].withInferredSchema(), children.value()[1].withInferredSchema()};
    copy.inferLocalSchema();
    return copy;
}

Reflected Reflector<TypedLogicalOperator<AsOfJoinLogicalOperator>>::operator()(
    const TypedLogicalOperator<AsOfJoinLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedAsOfJoinLogicalOperator{
        .operatorId = op.getId(),
        .joinFunction = op->getJoinFunction(),
        .timeCharacteristics = op->getTimeCharacteristics(),
        .rightIsTable = op->isRightTable()});
}

Unreflector<TypedLogicalOperator<AsOfJoinLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<AsOfJoinLogicalOperator>
Unreflector<TypedLogicalOperator<AsOfJoinLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    const auto data = context.unreflect<detail::ReflectedAsOfJoinLogicalOperator>(reflected);
    const auto children = plan->getChildrenFor(data.operatorId, context);
    PRECONDITION(children.size() == 2, "Reflected ASOF join requires exactly two children");
    return AsOfJoinLogicalOperator::create(
        std::array{children[0], children[1]}, data.joinFunction, data.timeCharacteristics, data.rightIsTable);
}

}

std::size_t std::hash<NES::AsOfJoinLogicalOperator>::operator()(const NES::AsOfJoinLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.joinFunction, op.rightIsTable);
}
