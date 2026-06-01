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

#include <Operators/InferModelLogicalOperator.hpp>

#include <algorithm>
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
#include <fmt/ranges.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

InferModelLogicalOperator::InferModelLogicalOperator(WeakLogicalOperator self, RegisteredModel model)
    : ManagedByOperator(std::move(self)), model(std::move(model))
{
}

InferModelLogicalOperator::InferModelLogicalOperator(WeakLogicalOperator self, RegisteredModel model, LogicalOperator child)
    : ManagedByOperator(std::move(self)), model(std::move(model)), child(std::move(child))
{
    inferLocalSchema();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view InferModelLogicalOperator::getName() const noexcept
{
    return NAME;
}

const RegisteredModel& InferModelLogicalOperator::getModel() const
{
    return model;
}

bool InferModelLogicalOperator::operator==(const InferModelLogicalOperator& rhs) const
{
    return model == rhs.model && getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string InferModelLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    const auto inputNames = model.getSchema().inputs
        | std::views::transform([](const UnqualifiedUnboundField& field) { return fmt::format("{}", field.getFullyQualifiedName()); });
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "INFER_MODEL(opId: {}, inputFields: [{}], traitSet: {})", opId, fmt::join(inputNames, ", "), traitSet.explain(verbosity));
    }
    return fmt::format("INFER_MODEL(inputFields: [{}])", fmt::join(inputNames, ", "));
}

void InferModelLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "InferModel requires a child for local schema inference");
    const auto childOutput = child->getOutputSchema();

    const auto& modelInputs = model.getSchema().inputs;
    const auto& modelOutputs = model.getSchema().outputs;

    /// Verify the child's schema contains a matching, non-nullable, same-typed field for every model input.
    for (const auto& modelInput : modelInputs)
    {
        const auto field = childOutput[modelInput.getFullyQualifiedName()];
        if (!field.has_value())
        {
            throw CannotInferSchema("Field '{}' not found in input schema", modelInput.getFullyQualifiedName());
        }
        if (field->getDataType().nullable)
        {
            throw CannotInferSchema("Field '{}' is nullable, but model inputs must not be nullable", modelInput.getFullyQualifiedName());
        }
        if (field->getDataType().type != modelInput.getDataType().type)
        {
            throw CannotInferSchema(
                "Type mismatch for field '{}': schema has a different type than model expects", modelInput.getFullyQualifiedName());
        }
    }

    auto outputFields = childOutput | RangeUnbinder{} | std::ranges::to<std::vector>();
    std::ranges::copy(modelOutputs, std::back_inserter(outputFields));
    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "InferModel output schema has name collisions between child fields and model outputs: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = std::move(outputSchemaOrCollisions).value();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
InferModelLogicalOperator InferModelLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "InferModel requires a child");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet InferModelLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InferModelLogicalOperator InferModelLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

InferModelLogicalOperator InferModelLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for InferModel, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

InferModelLogicalOperator InferModelLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for InferModel, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> InferModelLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

Schema<Field, Ordered> InferModelLogicalOperator::getOrderedOutputSchema(const ChildOutputOrderProvider orderProvider) const
{
    PRECONDITION(child.has_value(), "InferModel requires a child to derive its ordered output schema");

    /// Concatenate child's ordered output and the model's ordered outputs; collisions are an error,
    std::vector<UnqualifiedUnboundField> fields = orderProvider(child.value()) | RangeUnbinder{} | std::ranges::to<std::vector>();
    std::ranges::copy(model.getSchema().outputs, std::back_inserter(fields));
    auto orderedOrCollisions = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!orderedOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "InferModel output schema has name collisions between child fields and model outputs: "
            + Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(orderedOrCollisions.error()));
    }
    return NES::bindToOperator(self.lock(), std::move(orderedOrCollisions).value());
}

std::vector<LogicalOperator> InferModelLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator InferModelLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<InferModelLogicalOperator>>::operator()(
    const TypedLogicalOperator<InferModelLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedInferModelLogicalOperator{.operatorId = op.getId(), .model = reflect(op->getModel())});
}

Unreflector<TypedLogicalOperator<InferModelLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<InferModelLogicalOperator>
Unreflector<TypedLogicalOperator<InferModelLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [operatorId, model] = context.unreflect<detail::ReflectedInferModelLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("InferModelLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return TypedLogicalOperator<InferModelLogicalOperator>{context.unreflect<RegisteredModel>(model), std::move(children.at(0))};
}

}

std::size_t std::hash<NES::InferModelLogicalOperator>::operator()(const NES::InferModelLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getModel().getName());
}
