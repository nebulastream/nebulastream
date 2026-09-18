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

#include <Operators/SemMapNameLogicalOperator.hpp>

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SemMapNameLogicalOperator::SemMapNameLogicalOperator(
    WeakLogicalOperator self,
    std::string modelName,
    std::vector<UnqualifiedUnboundField> callSiteInputs,
    std::optional<Identifier> outputAlias)
    : ManagedByOperator(std::move(self))
    , modelName(std::move(modelName))
    , callSiteInputs(std::move(callSiteInputs))
    , outputAlias(std::move(outputAlias))
{
}

SemMapNameLogicalOperator::SemMapNameLogicalOperator(
    WeakLogicalOperator self,
    std::string modelName,
    std::vector<UnqualifiedUnboundField> callSiteInputs,
    LogicalOperator child,
    std::optional<Identifier> outputAlias)
    : ManagedByOperator(std::move(self))
    , modelName(std::move(modelName))
    , callSiteInputs(std::move(callSiteInputs))
    , outputAlias(std::move(outputAlias))
    , child(std::move(child))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view SemMapNameLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string SemMapNameLogicalOperator::getModelName() const
{
    return modelName;
}

const std::vector<UnqualifiedUnboundField>& SemMapNameLogicalOperator::getCallSiteInputs() const
{
    return callSiteInputs;
}

const std::optional<Identifier>& SemMapNameLogicalOperator::getOutputAlias() const
{
    return outputAlias;
}

bool SemMapNameLogicalOperator::operator==(const SemMapNameLogicalOperator& rhs) const
{
    return modelName == rhs.modelName && callSiteInputs == rhs.callSiteInputs && outputAlias == rhs.outputAlias
        && getTraitSet() == rhs.getTraitSet();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string SemMapNameLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    const auto inputNames = callSiteInputs
        | std::views::transform([](const UnqualifiedUnboundField& field) { return fmt::format("{}", field.getFullyQualifiedName()); });
    const auto aliasSuffix = outputAlias.has_value() ? fmt::format(", outputAlias: {}", *outputAlias) : std::string{};
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SEM_MAP_NAME(opId: {}, model: {}, inputFields: [{}]{}, traitSet: {})",
            opId,
            modelName,
            fmt::join(inputNames, ", "),
            aliasSuffix,
            traitSet.explain(verbosity));
    }
    return fmt::format("SEM_MAP_NAME(model: {}, inputFields: [{}]{})", modelName, fmt::join(inputNames, ", "), aliasSuffix);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
SemMapNameLogicalOperator SemMapNameLogicalOperator::withInferredSchema() const
{
    PRECONDITION(false, "SemMapName requires model resolution before schema inference");
    std::unreachable();
}

TraitSet SemMapNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SemMapNameLogicalOperator SemMapNameLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

SemMapNameLogicalOperator SemMapNameLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for SemMapName, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
/// Generic plan-rewriting rules (e.g. InferModelResolutionRule) call withChildren on every operator
/// while traversing, before SemMapResolutionRule replaces the name variant — so this must actually
/// set the child instead of guarding. Schema inference stays guarded (requires model resolution).
SemMapNameLogicalOperator SemMapNameLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for SemMapName, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema<Field, Unordered> SemMapNameLogicalOperator::getOutputSchema() const
{
    PRECONDITION(false, "SemMapName requires model resolution and schema inference before retrieving output schema");
    std::unreachable();
}

Schema<Field, Ordered> SemMapNameLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider) const
{
    PRECONDITION(false, "SemMapName requires model resolution before ordered schema inference");
    std::unreachable();
}

std::vector<LogicalOperator> SemMapNameLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator SemMapNameLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<SemMapNameLogicalOperator>>::operator()(
    const TypedLogicalOperator<SemMapNameLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedSemMapNameLogicalOperator{
        .operatorId = op.getId(),
        .modelName = std::make_optional(op->getModelName()),
        .callSiteInputs = std::make_optional(op->getCallSiteInputs()),
        .outputAlias = op->getOutputAlias()});
}

Unreflector<TypedLogicalOperator<SemMapNameLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<SemMapNameLogicalOperator>
Unreflector<TypedLogicalOperator<SemMapNameLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedSemMapNameLogicalOperator>(rfl);

    if (!reflected.modelName.has_value())
    {
        throw CannotDeserialize("Failed to deserialize TypedLogicalOperator<SemMapNameLogicalOperator>");
    }

    auto children = plan->getChildrenFor(reflected.operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("SemMapNameLogicalOperator requires exactly one child, but got {}", children.size());
    }

    return TypedLogicalOperator<SemMapNameLogicalOperator>{
        std::move(reflected.modelName).value(),
        reflected.callSiteInputs.value_or(std::vector<UnqualifiedUnboundField>{}),
        std::move(children.at(0)),
        reflected.outputAlias};
}

}

std::size_t std::hash<NES::SemMapNameLogicalOperator>::operator()(const NES::SemMapNameLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getModelName());
}
