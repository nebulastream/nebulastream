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

#include <Operators/Windows/IntervalJoinLogicalOperator.hpp>

#include <array>
#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Hash.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

namespace
{
void validateBounds(const int64_t lowerBound, const int64_t upperBound)
{
    if (lowerBound >= upperBound)
    {
        throw InvalidQuerySyntax(
            "IntervalJoin requires lowerBound < upperBound; got lowerBound={}, upperBound={}.", lowerBound, upperBound);
    }
}
}

IntervalJoinLogicalOperator::IntervalJoinLogicalOperator(
    WeakLogicalOperator self,
    LogicalFunction joinFunction,
    JoinTimeCharacteristic timeCharacteristics,
    const int64_t lowerBound,
    const int64_t upperBound,
    const JoinLogicalOperator::JoinType joinType)
    : ManagedByOperator(std::move(self))
    , joinFunction(std::move(joinFunction))
    , timestampFields(std::move(timeCharacteristics))
    , lowerBound(lowerBound)
    , upperBound(upperBound)
    , joinType(joinType)
{
    validateBounds(lowerBound, upperBound);
}

IntervalJoinLogicalOperator::IntervalJoinLogicalOperator(
    WeakLogicalOperator self,
    std::array<LogicalOperator, 2> children,
    LogicalFunction joinFunction,
    JoinTimeCharacteristic timeCharacteristics,
    const int64_t lowerBound,
    const int64_t upperBound,
    const JoinLogicalOperator::JoinType joinType)
    : ManagedByOperator(std::move(self))
    , joinFunction(std::move(joinFunction))
    , timestampFields(std::move(timeCharacteristics))
    , lowerBound(lowerBound)
    , upperBound(upperBound)
    , joinType(joinType)
    , children(std::move(children))
{
    validateBounds(lowerBound, upperBound);
    inferLocalSchema();
}

std::string_view IntervalJoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool IntervalJoinLogicalOperator::operator==(const IntervalJoinLogicalOperator& rhs) const
{
    return timestampFields == rhs.timestampFields and lowerBound == rhs.lowerBound and upperBound == rhs.upperBound
        and joinType == rhs.joinType and getJoinFunction() == rhs.getJoinFunction() and outputSchema == rhs.outputSchema
        and getTraitSet() == rhs.getTraitSet();
}

std::string IntervalJoinLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "IntervalJoin(opId: {}, lowerBound: {} ms, upperBound: {} ms, joinFunction: {}, windowMetadata: (startField: {}, endField: "
            "{}), "
            "traitSet: {})",
            id,
            lowerBound,
            upperBound,
            getJoinFunction().explain(verbosity),
            startEndFields.at(0),
            startEndFields.at(1),
            traitSet.explain(verbosity));
    }
    return fmt::format("IntervalJoin({})", getJoinFunction().explain(verbosity));
}

void IntervalJoinLogicalOperator::inferLocalSchema()
{
    PRECONDITION(children.has_value(), "Child not set when calling schema inference");

    /// The predicate may still be unbound at first inference, so accept either the bound or unbound field-access
    /// function as evidence of an explicit equi-join predicate (mirrors JoinLogicalOperator).
    if (isOuterJoin(this->joinType)
        && std::ranges::none_of(
            BFSRange(this->joinFunction),
            [](const LogicalFunction& fn)
            {
                return fn.tryGetAs<FieldAccessLogicalFunction>().has_value()
                    || fn.tryGetAs<UnboundFieldAccessLogicalFunction>().has_value();
            }))
    {
        throw UnsupportedQuery("Outer interval joins require an explicit equi-join predicate (e.g. ON left$key = right$key)");
    }

    const std::vector<Field> inputFields = *children | std::views::transform([](const auto& child) { return child.getOutputSchema(); })
        | std::views::join | std::ranges::to<std::vector>();

    auto inputSchemaOrCollisions = Schema<Field, Unordered>::tryCreateCollisionFree(inputFields);
    if (!inputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in input schemas: " + Schema<Field, Unordered>::createCollisionString(inputSchemaOrCollisions.error()));
    }
    const auto& inputSchema = inputSchemaOrCollisions.value();

    this->joinFunction = this->joinFunction.withInferredDataType(inputSchema);

    /// Bind the per-side timestamp characteristics. The left timestamp is taken from the left input
    /// and the right timestamp from the right input; both are bound against the (collision-free)
    /// merged input schema by their distinct field names.
    this->timestampFields = std::visit(
        [&](const auto& tsFields)
        {
            return std::array{
                Windowing::TimeCharacteristicWrapper{tsFields[0]}.withInferredSchema(inputSchema),
                Windowing::TimeCharacteristicWrapper{tsFields[1]}.withInferredSchema(inputSchema)};
        },
        this->timestampFields);

    /// For outer joins, the fields of the side that may be unmatched become nullable in the output schema.
    const bool leftNullable
        = (joinType == JoinLogicalOperator::JoinType::OUTER_RIGHT_JOIN || joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN);
    const bool rightNullable
        = (joinType == JoinLogicalOperator::JoinType::OUTER_LEFT_JOIN || joinType == JoinLogicalOperator::JoinType::OUTER_FULL_JOIN);

    const auto toOutputField = [](const Field& field, const bool nullable) -> UnqualifiedUnboundField
    {
        if (nullable)
        {
            return UnqualifiedUnboundField{
                field.getFullyQualifiedName(), DataType{field.getDataType().type, DataType::NULLABLE::IS_NULLABLE}};
        }
        return field.unbound();
    };

    std::vector<UnqualifiedUnboundField> outputFields;
    for (const auto& field : (*children)[0].getOutputSchema())
    {
        outputFields.emplace_back(toOutputField(field, leftNullable));
    }
    for (const auto& field : (*children)[1].getOutputSchema())
    {
        outputFields.emplace_back(toOutputField(field, rightNullable));
    }
    outputFields.emplace_back(startEndFields[0].getFullyQualifiedName(), startEndFields[0].getDataType());
    outputFields.emplace_back(startEndFields[1].getFullyQualifiedName(), startEndFields[1].getDataType());

    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in interval-join output schema: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    this->outputSchema = outputSchemaOrCollisions.value();
}

IntervalJoinLogicalOperator IntervalJoinLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.children = {(*copy.children)[0].withInferredSchema(), (*copy.children)[1].withInferredSchema()};
    copy.inferLocalSchema();
    return copy;
}

IntervalJoinLogicalOperator IntervalJoinLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet IntervalJoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

IntervalJoinLogicalOperator IntervalJoinLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 2, "Can only set exactly two children for interval join, got {}", children.size());
    auto copy = *this;
    copy.children = std::array{std::move(children.at(0)), std::move(children.at(1))};
    return copy;
}

IntervalJoinLogicalOperator IntervalJoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 2, "Can only set exactly two children for interval join, got {}", children.size());
    auto copy = *this;
    copy.children = std::array{std::move(children.at(0)), std::move(children.at(1))};
    copy.inferLocalSchema();
    return copy;
}

std::vector<LogicalOperator> IntervalJoinLogicalOperator::getChildren() const
{
    if (children.has_value())
    {
        return *children | std::ranges::to<std::vector>();
    }
    return {};
}

std::array<LogicalOperator, 2> IntervalJoinLogicalOperator::getBothChildren() const
{
    PRECONDITION(children.has_value(), "Children not set when trying to retrieve interval-join children");
    return *children;
}

Schema<Field, Unordered> IntervalJoinLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

const UnqualifiedUnboundField& IntervalJoinLogicalOperator::getStartField() const
{
    return startEndFields[0];
}

const UnqualifiedUnboundField& IntervalJoinLogicalOperator::getEndField() const
{
    return startEndFields[1];
}

JoinTimeCharacteristic IntervalJoinLogicalOperator::getJoinTimeCharacteristics() const
{
    return timestampFields;
}

int64_t IntervalJoinLogicalOperator::getLowerBound() const noexcept
{
    return lowerBound;
}

int64_t IntervalJoinLogicalOperator::getUpperBound() const noexcept
{
    return upperBound;
}

JoinLogicalOperator::JoinType IntervalJoinLogicalOperator::getJoinType() const noexcept
{
    return joinType;
}

LogicalFunction IntervalJoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

Reflected
Reflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>::operator()(const TypedLogicalOperator<IntervalJoinLogicalOperator>& op) const
{
    return reflect(detail::ReflectedIntervalJoinLogicalOperator{
        .operatorId = op.getId(),
        .joinFunction = op->getJoinFunction(),
        .timestampFields = op->getJoinTimeCharacteristics(),
        .lowerBound = op->getLowerBound(),
        .upperBound = op->getUpperBound(),
        .joinType = op->getJoinType()});
}

Unreflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<IntervalJoinLogicalOperator> Unreflector<TypedLogicalOperator<IntervalJoinLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [operatorId, joinFunction, timestampFields, lowerBound, upperBound, joinType]
        = context.unreflect<detail::ReflectedIntervalJoinLogicalOperator>(reflected);
    auto foundChildren = plan->getChildrenFor(operatorId, context);
    return TypedLogicalOperator<IntervalJoinLogicalOperator>{
        std::array{foundChildren.at(0), foundChildren.at(1)},
        std::move(joinFunction),
        std::move(timestampFields),
        lowerBound,
        upperBound,
        joinType};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterIntervalJoinLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<TypedLogicalOperator<IntervalJoinLogicalOperator>>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}

std::size_t
std::hash<NES::IntervalJoinLogicalOperator>::operator()(const NES::IntervalJoinLogicalOperator& intervalJoinLogicalOperator) const noexcept
{
    return folly::hash::hash_combine_generic(
        NES::Hash{},
        intervalJoinLogicalOperator.joinType,
        intervalJoinLogicalOperator.joinFunction,
        intervalJoinLogicalOperator.lowerBound,
        intervalJoinLogicalOperator.upperBound,
        intervalJoinLogicalOperator.startEndFields);
}
