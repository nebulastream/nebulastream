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

#include <Operators/MarkApplyLogicalOperator.hpp>

#include <array>
#include <functional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/BooleanFunctions/EqualsLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/Reorderer.hpp>
#include <Schema/Binder.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/Hash.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{
namespace
{
std::vector<Identifier> createTableFieldNames(const Identifier& markField, const size_t count)
{
    return std::views::iota(size_t{0}, count)
        | std::views::transform(
            [&markField](const size_t index)
            { return Identifier::parse(fmt::format("__nes_mark_apply_table_{}_{}", markField, index)); })
        | std::ranges::to<std::vector>();
}

Schema<Field, Ordered> getOrderedOutputSchema(const LogicalOperator& visiting)
{
    const auto childrenWithOutputOrder = visiting.getChildren()
        | std::views::transform([](const auto& child) { return std::pair{child, getOrderedOutputSchema(child)}; })
        | std::ranges::to<std::unordered_map>();
    if (const auto reorderer = visiting.tryGetAs<Reorderer>())
    {
        return reorderer.value()->get().getOrderedOutputSchema(
            [&childrenWithOutputOrder](const LogicalOperator& child) { return childrenWithOutputOrder.at(child); });
    }
    const auto outputSchema = visiting.getOutputSchema();
    std::vector<Field> ordered;
    for (const auto& child : visiting.getChildren())
    {
        for (const auto& field : childrenWithOutputOrder.at(child))
        {
            if (const auto output = outputSchema[field.getFullyQualifiedName()])
            {
                ordered.push_back(output.value());
            }
        }
    }
    for (const auto& field : outputSchema)
    {
        if (!std::ranges::contains(ordered, field))
        {
            ordered.push_back(field);
        }
    }
    return ordered | std::ranges::to<Schema<Field, Ordered>>();
}
}

MarkApplyLogicalOperator::MarkApplyLogicalOperator(
    WeakLogicalOperator self,
    std::vector<LogicalFunction> probeValues,
    Identifier markField,
    std::vector<Identifier> correlationFields)
    : ManagedByOperator(std::move(self))
    , probeValues(std::move(probeValues))
    , markField(std::move(markField))
    , correlationFields(std::move(correlationFields))
    , tableFieldNames(createTableFieldNames(this->markField, this->probeValues.size()))
{
    PRECONDITION(!this->probeValues.empty(), "Mark apply requires at least one probe value");
}

MarkApplyLogicalOperator::MarkApplyLogicalOperator(
    WeakLogicalOperator self,
    std::array<LogicalOperator, 2> children,
    std::vector<LogicalFunction> probeValues,
    Identifier markField,
    std::vector<Identifier> correlationFields)
    : ManagedByOperator(std::move(self))
    , children(std::move(children))
    , probeValues(std::move(probeValues))
    , markField(std::move(markField))
    , correlationFields(std::move(correlationFields))
    , tableFieldNames(createTableFieldNames(this->markField, this->probeValues.size()))
{
    PRECONDITION(!this->probeValues.empty(), "Mark apply requires at least one probe value");
    inferLocalSchema();
}

TypedLogicalOperator<MarkApplyLogicalOperator> MarkApplyLogicalOperator::create(
    std::vector<LogicalFunction> probeValues, Identifier markField, std::vector<Identifier> correlationFields)
{
    return TypedLogicalOperator<MarkApplyLogicalOperator>{
        std::move(probeValues), std::move(markField), std::move(correlationFields)};
}

TypedLogicalOperator<MarkApplyLogicalOperator> MarkApplyLogicalOperator::create(
    std::array<LogicalOperator, 2> children,
    std::vector<LogicalFunction> probeValues,
    Identifier markField,
    std::vector<Identifier> correlationFields)
{
    return TypedLogicalOperator<MarkApplyLogicalOperator>{
        std::move(children), std::move(probeValues), std::move(markField), std::move(correlationFields)};
}

void MarkApplyLogicalOperator::inferLocalSchema()
{
    PRECONDITION(children.has_value(), "Children not set when inferring mark apply schema");
    if (!correlationFields.empty())
    {
        throw UnsupportedQuery("Correlated mark apply is not implemented yet");
    }

    const auto inputSchema = children.value()[0].getOutputSchema();
    auto tableFields = getOrderedOutputSchema(children.value()[1]) | std::ranges::to<std::vector>();
    if (tableFields.size() != probeValues.size())
    {
        throw CannotInferSchema(
            "IN probe has {} columns but its subquery returns {} columns", probeValues.size(), tableFields.size());
    }

    const auto alreadyRenamed = std::ranges::equal(
        tableFields | std::views::transform([](const Field& field) { return field.getLastName(); }), tableFieldNames);
    if (!alreadyRenamed)
    {
        auto projections = std::views::zip(tableFieldNames, tableFields)
            | std::views::transform(
                [](const auto& pair) -> ProjectionLogicalOperator::UnboundProjection
                { return {std::get<0>(pair), FieldAccessLogicalFunction{std::get<1>(pair)}}; })
            | std::ranges::to<std::vector>();
        children.value()[1] = ProjectionLogicalOperator::create(
            children.value()[1], std::move(projections), ProjectionLogicalOperator::Asterisk{false});
        tableFields = getOrderedOutputSchema(children.value()[1]) | std::ranges::to<std::vector>();
    }

    probeValues = probeValues
        | std::views::transform([&inputSchema](const LogicalFunction& value) { return value.withInferredDataType(inputSchema); })
        | std::ranges::to<std::vector>();
    LogicalFunction comparison = EqualsLogicalFunction(probeValues.front(), FieldAccessLogicalFunction{tableFields.front()});
    for (size_t index = 1; index < probeValues.size(); ++index)
    {
        comparison = AndLogicalFunction(
            std::move(comparison), EqualsLogicalFunction(probeValues.at(index), FieldAccessLogicalFunction{tableFields.at(index)}));
    }
    const auto comparisonSchema = Schema<Field, Unordered>{
        std::vector{inputSchema | std::ranges::to<std::vector>(), tableFields} | std::views::join | std::ranges::to<std::vector>()};
    comparisonFunction = comparison.withInferredDataType(comparisonSchema);

    auto outputFields = inputSchema | std::views::transform([](const Field& field) { return field.unbound(); })
        | std::ranges::to<std::vector>();
    outputFields.emplace_back(
        markField, DataTypeProvider::provideDataType(DataType::Type::BOOLEAN, DataType::NULLABLE::IS_NULLABLE));
    auto inferredOutputSchema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!inferredOutputSchema.has_value())
    {
        throw CannotInferSchema("Mark field {} collides with its input schema", markField);
    }
    outputSchema = std::move(inferredOutputSchema).value();
}

const std::vector<LogicalFunction>& MarkApplyLogicalOperator::getProbeValues() const
{
    return probeValues;
}

LogicalFunction MarkApplyLogicalOperator::getComparisonFunction() const
{
    PRECONDITION(comparisonFunction.has_value(), "Mark apply comparison accessed before schema inference");
    return comparisonFunction.value();
}

Identifier MarkApplyLogicalOperator::getMarkField() const
{
    return markField;
}

const std::vector<Identifier>& MarkApplyLogicalOperator::getCorrelationFields() const
{
    return correlationFields;
}

std::array<LogicalOperator, 2> MarkApplyLogicalOperator::getBothChildren() const
{
    PRECONDITION(children.has_value(), "Mark apply children are not set");
    return children.value();
}

bool MarkApplyLogicalOperator::operator==(const MarkApplyLogicalOperator& rhs) const
{
    return probeValues == rhs.probeValues && markField == rhs.markField && correlationFields == rhs.correlationFields
        && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

MarkApplyLogicalOperator MarkApplyLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

TraitSet MarkApplyLogicalOperator::getTraitSet() const
{
    return traitSet;
}

Schema<Field, Unordered> MarkApplyLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed mark apply schema before inference");
    return bindToOperator(self.lock(), outputSchema.value());
}

MarkApplyLogicalOperator MarkApplyLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "Mark apply requires exactly two children");
    auto copy = *this;
    copy.children = std::array{std::move(newChildren[0]), std::move(newChildren[1])};
    return copy;
}

MarkApplyLogicalOperator MarkApplyLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = withChildrenUnsafe(std::move(newChildren));
    copy.inferLocalSchema();
    return copy;
}

std::vector<LogicalOperator> MarkApplyLogicalOperator::getChildren() const
{
    return children.has_value() ? children.value() | std::ranges::to<std::vector>() : std::vector<LogicalOperator>{};
}

std::string MarkApplyLogicalOperator::explain(const ExplainVerbosity verbosity, const OperatorId id) const
{
    return verbosity == ExplainVerbosity::Debug
        ? fmt::format("MarkApply(opId: {}, mark: {}, probes: {}, traitSet: {})", id, markField, probeValues, traitSet.explain(verbosity))
        : fmt::format("MarkApply({}, {})", markField, probeValues);
}

std::string_view MarkApplyLogicalOperator::getName() noexcept
{
    return NAME;
}

MarkApplyLogicalOperator MarkApplyLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.has_value(), "Children not set when inferring mark apply schema");
    auto copy = *this;
    copy.children = std::array{children.value()[0].withInferredSchema(), children.value()[1].withInferredSchema()};
    copy.inferLocalSchema();
    return copy;
}

Reflected Reflector<TypedLogicalOperator<MarkApplyLogicalOperator>>::operator()(
    const TypedLogicalOperator<MarkApplyLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedMarkApplyLogicalOperator{
        .operatorId = op.getId(),
        .probeValues = op->getProbeValues(),
        .markField = op->getMarkField(),
        .correlationFields = op->getCorrelationFields()});
}

Unreflector<TypedLogicalOperator<MarkApplyLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<MarkApplyLogicalOperator> Unreflector<TypedLogicalOperator<MarkApplyLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    const auto data = context.unreflect<detail::ReflectedMarkApplyLogicalOperator>(reflected);
    const auto children = plan->getChildrenFor(data.operatorId, context);
    PRECONDITION(children.size() == 2, "Reflected mark apply requires exactly two children");
    return MarkApplyLogicalOperator::create(
        std::array{children[0], children[1]}, data.probeValues, data.markField, data.correlationFields);
}

}

std::size_t std::hash<NES::MarkApplyLogicalOperator>::operator()(const NES::MarkApplyLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.probeValues, op.markField, op.correlationFields);
}
