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

#include <Operators/SemMapLogicalOperator.hpp>

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
#include <DataTypes/UnboundField.hpp>
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
#include <SemanticModelCatalog.hpp>

namespace NES
{

SemMapLogicalOperator::SemMapLogicalOperator(
    WeakLogicalOperator self,
    RegisteredSemanticModel model,
    std::vector<UnqualifiedUnboundField> callSiteInputs,
    std::optional<Identifier> outputAlias)
    : ManagedByOperator(std::move(self))
    , model(std::move(model))
    , callSiteInputs(std::move(callSiteInputs))
    , outputAlias(std::move(outputAlias))
{
}

SemMapLogicalOperator::SemMapLogicalOperator(
    WeakLogicalOperator self,
    RegisteredSemanticModel model,
    std::vector<UnqualifiedUnboundField> callSiteInputs,
    LogicalOperator child,
    std::optional<Identifier> outputAlias)
    : ManagedByOperator(std::move(self))
    , model(std::move(model))
    , callSiteInputs(std::move(callSiteInputs))
    , outputAlias(std::move(outputAlias))
    , child(std::move(child))
{
    inferLocalSchema();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view SemMapLogicalOperator::getName() const noexcept
{
    return NAME;
}

const RegisteredSemanticModel& SemMapLogicalOperator::getModel() const
{
    return model;
}

const std::vector<UnqualifiedUnboundField>& SemMapLogicalOperator::getCallSiteInputs() const
{
    return callSiteInputs;
}

const std::optional<Identifier>& SemMapLogicalOperator::getOutputAlias() const
{
    return outputAlias;
}

bool SemMapLogicalOperator::operator==(const SemMapLogicalOperator& rhs) const
{
    return model == rhs.model && callSiteInputs == rhs.callSiteInputs && outputAlias == rhs.outputAlias
        && getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string SemMapLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    const auto inputNames
        = callSiteInputs | std::views::transform([](const UnqualifiedUnboundField& field) { return fmt::format("{}", field.getFullyQualifiedName()); });
    const auto aliasSuffix = outputAlias.has_value() ? fmt::format(", outputAlias: {}", *outputAlias) : std::string{};
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SEM_MAP(opId: {}, model: {}, inputFields: [{}]{}, traitSet: {})",
            opId,
            model.getName(),
            fmt::join(inputNames, ", "),
            aliasSuffix,
            traitSet.explain(verbosity));
    }
    return fmt::format("SEM_MAP(model: {}, inputFields: [{}]{})", model.getName(), fmt::join(inputNames, ", "), aliasSuffix);
}

void SemMapLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "SemMap requires a child for local schema inference");
    const auto childOutput = child->getOutputSchema();

    const auto& catalogInputs = model.getSchema().inputs;
    if (callSiteInputs.size() != catalogInputs.size())
    {
        throw CannotInferSchema(
            "SemMap call site provides {} input field(s), but model '{}' declares {}",
            callSiteInputs.size(),
            model.getName(),
            catalogInputs.size());
    }

    /// Verify the child's schema contains a matching, non-nullable, VARSIZED field for every
    /// call-site input. VARSIZED is looser than InferModel's exact-type match — inputs get
    /// stringified before being sent to the LLM anyway.
    for (const auto& callSiteInput : callSiteInputs)
    {
        const auto field = childOutput[callSiteInput.getFullyQualifiedName()];
        if (!field.has_value())
        {
            throw CannotInferSchema("Field '{}' not found in input schema", callSiteInput.getFullyQualifiedName());
        }
        if (field->getDataType().nullable)
        {
            throw CannotInferSchema("Field '{}' is nullable, but SemMap inputs must not be nullable", callSiteInput.getFullyQualifiedName());
        }
        if (!field->getDataType().isType(DataType::Type::VARSIZED))
        {
            throw CannotInferSchema("Field '{}' must be VARSIZED for SemMap", callSiteInput.getFullyQualifiedName());
        }
    }

    const auto resolvedModelOutputs = resolvedModelOutputFields();
    auto outputFields = childOutput | RangeUnbinder{} | std::ranges::to<std::vector>();
    std::ranges::copy(resolvedModelOutputs, std::back_inserter(outputFields));
    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "SemMap output schema has name collisions between child fields and model outputs: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = std::move(outputSchemaOrCollisions).value();
}

std::vector<UnqualifiedUnboundField> SemMapLogicalOperator::resolvedModelOutputFields() const
{
    const auto& modelOutputs = model.getSchema().outputs;
    if (!outputAlias.has_value())
    {
        return modelOutputs | std::ranges::to<std::vector>();
    }
    if (modelOutputs.size() != 1)
    {
        throw CannotInferSchema(
            "SEM_MAP alias '{}' requires a model with exactly one OUTPUT field, but '{}' declares {}",
            *outputAlias,
            model.getName(),
            modelOutputs.size());
    }
    const auto& singleOutput = *modelOutputs.begin();
    return {UnqualifiedUnboundField{*outputAlias, singleOutput.getDataType()}};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
SemMapLogicalOperator SemMapLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "SemMap requires a child");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet SemMapLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SemMapLogicalOperator SemMapLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

SemMapLogicalOperator SemMapLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for SemMap, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

SemMapLogicalOperator SemMapLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for SemMap, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> SemMapLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

Schema<Field, Ordered> SemMapLogicalOperator::getOrderedOutputSchema(const ChildOutputOrderProvider orderProvider) const
{
    PRECONDITION(child.has_value(), "SemMap requires a child to derive its ordered output schema");

    std::vector<UnqualifiedUnboundField> fields = orderProvider(child.value()) | RangeUnbinder{} | std::ranges::to<std::vector>();
    std::ranges::copy(resolvedModelOutputFields(), std::back_inserter(fields));
    auto orderedOrCollisions = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!orderedOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "SemMap output schema has name collisions between child fields and model outputs: "
            + Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(orderedOrCollisions.error()));
    }
    return NES::bindToOperator(self.lock(), std::move(orderedOrCollisions).value());
}

std::vector<LogicalOperator> SemMapLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator SemMapLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<SemMapLogicalOperator>>::operator()(
    const TypedLogicalOperator<SemMapLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedSemMapLogicalOperator{
        .operatorId = op.getId(),
        .model = context.reflect(op->getModel()),
        .callSiteInputs = op->getCallSiteInputs(),
        .outputAlias = op->getOutputAlias()});
}

Unreflector<TypedLogicalOperator<SemMapLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<SemMapLogicalOperator>
Unreflector<TypedLogicalOperator<SemMapLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedSemMapLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(reflected.operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("SemMapLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return TypedLogicalOperator<SemMapLogicalOperator>{
        context.unreflect<RegisteredSemanticModel>(reflected.model),
        reflected.callSiteInputs.value_or(std::vector<UnqualifiedUnboundField>{}),
        std::move(children.at(0)),
        reflected.outputAlias};
}

}

std::size_t std::hash<NES::SemMapLogicalOperator>::operator()(const NES::SemMapLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getModel().getName());
}
