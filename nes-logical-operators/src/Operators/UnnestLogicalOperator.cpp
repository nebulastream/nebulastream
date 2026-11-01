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


#include <Operators/UnnestLogicalOperator.hpp>

#include <cstddef>
#include <functional>
#include <iterator>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <fmt/format.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

UnnestLogicalOperator::UnnestLogicalOperator(WeakLogicalOperator self, Identifier input, Identifier output)
    : ManagedByOperator(std::move(self)), input(std::move(input)), output(std::move(output))
{
}

UnnestLogicalOperator::UnnestLogicalOperator(WeakLogicalOperator self, LogicalOperator child, Identifier input, Identifier output)
    : ManagedByOperator(std::move(self)), input(std::move(input)), output(std::move(output)), child(std::move(child))
{
    inferLocalSchema();
}

TypedLogicalOperator<UnnestLogicalOperator> UnnestLogicalOperator::create(LogicalOperator child, Identifier input, Identifier output)
{
    return TypedLogicalOperator<UnnestLogicalOperator>{std::move(child), std::move(input), std::move(output)};
}

const Identifier& UnnestLogicalOperator::getInput() const
{
    return input;
}

const Identifier& UnnestLogicalOperator::getOutput() const
{
    return output;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view UnnestLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool UnnestLogicalOperator::operator==(const UnnestLogicalOperator& rhs) const
{
    return input == rhs.input && output == rhs.output && getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

std::string UnnestLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("UNNEST(opId: {}, {} as {}, traitSet: {})", opId, input, output, traitSet.explain(verbosity));
    }
    return fmt::format("UNNEST({} as {})", input, output);
}

void UnnestLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Unnest requires a child for local schema inference");
    const auto childOutput = child->getOutputSchema();
    const auto field = childOutput.getFieldByName(input);
    if (!field.has_value())
    {
        throw CannotInferSchema("UNNEST reads field '{}', which its input does not have", input);
    }
    if (!field->getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("UNNEST expects a VARSIZED field holding a JSON array, but '{}' is {}", input, field->getDataType());
    }
    if (field->getDataType().nullable)
    {
        throw UnsupportedQuery("UNNEST does not support the nullable field '{}'", input);
    }

    auto outputFields = childOutput | RangeUnbinder{} | std::ranges::to<std::vector>();
    outputFields.emplace_back(output, DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "UNNEST output field collides with an input field: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = std::move(outputSchemaOrCollisions).value();
}

UnnestLogicalOperator UnnestLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Unnest requires a child");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet UnnestLogicalOperator::getTraitSet() const
{
    return traitSet;
}

UnnestLogicalOperator UnnestLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

UnnestLogicalOperator UnnestLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for Unnest, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

UnnestLogicalOperator UnnestLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for Unnest, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> UnnestLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

Schema<Field, Ordered> UnnestLogicalOperator::getOrderedOutputSchema(const ChildOutputOrderProvider orderProvider) const
{
    PRECONDITION(child.has_value(), "Unnest requires a child to derive its ordered output schema");
    std::vector<UnqualifiedUnboundField> fields = orderProvider(child.value()) | RangeUnbinder{} | std::ranges::to<std::vector>();
    fields.emplace_back(output, DataTypeProvider::provideDataType(DataType::Type::VARSIZED));
    auto orderedOrCollisions = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!orderedOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "UNNEST output field collides with an input field: "
            + Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(orderedOrCollisions.error()));
    }
    return NES::bindToOperator(self.lock(), std::move(orderedOrCollisions).value());
}

std::vector<LogicalOperator> UnnestLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator UnnestLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<UnnestLogicalOperator>>::operator()(
    const TypedLogicalOperator<UnnestLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedUnnestLogicalOperator{
            .operatorId = op.getId(), .input = context.reflect(op->getInput()), .output = context.reflect(op->getOutput())});
}

Unreflector<TypedLogicalOperator<UnnestLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<UnnestLogicalOperator>
Unreflector<TypedLogicalOperator<UnnestLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [operatorId, input, output] = context.unreflect<detail::ReflectedUnnestLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("UnnestLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return TypedLogicalOperator<UnnestLogicalOperator>{
        std::move(children.at(0)), context.unreflect<Identifier>(input), context.unreflect<Identifier>(output)};
}

}

std::size_t std::hash<NES::UnnestLogicalOperator>::operator()(const NES::UnnestLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getInput().asCanonicalString() + "\n" + op.getOutput().asCanonicalString());
}
