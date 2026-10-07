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

#include <Operators/SemanticFilterNameLogicalOperator.hpp>

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

SemanticFilterNameLogicalOperator::SemanticFilterNameLogicalOperator(WeakLogicalOperator self, std::string modelName)
    : ManagedByOperator(std::move(self)), modelName(std::move(modelName))
{
}

SemanticFilterNameLogicalOperator::SemanticFilterNameLogicalOperator(WeakLogicalOperator self, std::string modelName, LogicalOperator child)
    : ManagedByOperator(std::move(self)), modelName(std::move(modelName)), child(std::move(child))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view SemanticFilterNameLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string SemanticFilterNameLogicalOperator::getModelName() const
{
    return modelName;
}

bool SemanticFilterNameLogicalOperator::operator==(const SemanticFilterNameLogicalOperator& rhs) const
{
    return modelName == rhs.modelName && getTraitSet() == rhs.getTraitSet();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string SemanticFilterNameLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SEM_FILTER_NAME(opId: {}, model: {}, traitSet: {})", opId, modelName, traitSet.explain(verbosity));
    }
    return fmt::format("SEM_FILTER_NAME(model: {})", modelName);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
SemanticFilterNameLogicalOperator SemanticFilterNameLogicalOperator::withInferredSchema() const
{
    PRECONDITION(false, "SemanticFilterName requires model resolution before schema inference");
    std::unreachable();
}

TraitSet SemanticFilterNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SemanticFilterNameLogicalOperator SemanticFilterNameLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

SemanticFilterNameLogicalOperator SemanticFilterNameLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for SemanticFilterName, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

/// Generic plan-rewriting rules rebuild every operator they walk over via withChildren, so a
/// guard here turns an unresolved placeholder into process death rather than a query error
/// (mirror of InferModelNameLogicalOperator). Only schema inference stays guarded: it needs the resolved model.
SemanticFilterNameLogicalOperator SemanticFilterNameLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for SemanticFilterName, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema<Field, Unordered> SemanticFilterNameLogicalOperator::getOutputSchema() const
{
    PRECONDITION(false, "SemanticFilterName requires model resolution and schema inference before retrieving output schema");
    std::unreachable();
}

Schema<Field, Ordered> SemanticFilterNameLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider) const
{
    PRECONDITION(false, "SemanticFilterName requires model resolution before ordered schema inference");
    std::unreachable();
}

std::vector<LogicalOperator> SemanticFilterNameLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator SemanticFilterNameLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<SemanticFilterNameLogicalOperator>>::operator()(
    const TypedLogicalOperator<SemanticFilterNameLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedSemanticFilterNameLogicalOperator{.operatorId = op.getId(), .modelName = std::make_optional(op->getModelName())});
}

Unreflector<TypedLogicalOperator<SemanticFilterNameLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<SemanticFilterNameLogicalOperator> Unreflector<TypedLogicalOperator<SemanticFilterNameLogicalOperator>>::operator()(
    const Reflected& rfl, const ReflectionContext& context) const
{
    auto [operatorId, modelName] = context.unreflect<detail::ReflectedSemanticFilterNameLogicalOperator>(rfl);

    if (!modelName.has_value())
    {
        throw CannotDeserialize("Failed to deserialize TypedLogicalOperator<SemanticFilterNameLogicalOperator>");
    }

    auto children = plan->getChildrenFor(operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("SemanticFilterNameLogicalOperator requires exactly one child, but got {}", children.size());
    }

    return TypedLogicalOperator<SemanticFilterNameLogicalOperator>{modelName.value(), std::move(children.at(0))};
}

}

std::size_t std::hash<NES::SemanticFilterNameLogicalOperator>::operator()(const NES::SemanticFilterNameLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getModelName());
}
