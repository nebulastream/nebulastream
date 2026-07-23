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

#include <Operators/InferModelNameLogicalOperator.hpp>

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

InferModelNameLogicalOperator::InferModelNameLogicalOperator(WeakLogicalOperator self, std::string modelName)
    : ManagedByOperator(std::move(self)), modelName(std::move(modelName))
{
}

InferModelNameLogicalOperator::InferModelNameLogicalOperator(WeakLogicalOperator self, std::string modelName, LogicalOperator child)
    : ManagedByOperator(std::move(self)), modelName(std::move(modelName)), child(std::move(child))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view InferModelNameLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string InferModelNameLogicalOperator::getModelName() const
{
    return modelName;
}

bool InferModelNameLogicalOperator::operator==(const InferModelNameLogicalOperator& rhs) const
{
    return modelName == rhs.modelName && getTraitSet() == rhs.getTraitSet();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string InferModelNameLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INFER_MODEL_NAME(opId: {}, model: {}, traitSet: {})", opId, modelName, traitSet.explain(verbosity));
    }
    return fmt::format("INFER_MODEL_NAME(model: {})", modelName);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
InferModelNameLogicalOperator InferModelNameLogicalOperator::withInferredSchema() const
{
    PRECONDITION(false, "InferModelName requires model resolution before schema inference");
    std::unreachable();
}

TraitSet InferModelNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

InferModelNameLogicalOperator InferModelNameLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for InferModelName, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
InferModelNameLogicalOperator InferModelNameLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "InferModelName requires model resolution before schema inference");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema<Field, Unordered> InferModelNameLogicalOperator::getOutputSchema() const
{
    PRECONDITION(false, "InferModelName requires model resolution and schema inference before retrieving output schema");
    std::unreachable();
}

Schema<Field, Ordered> InferModelNameLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider) const
{
    PRECONDITION(false, "InferModelName requires model resolution before ordered schema inference");
    std::unreachable();
}

std::vector<LogicalOperator> InferModelNameLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator InferModelNameLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<InferModelNameLogicalOperator>>::operator()(
    const TypedLogicalOperator<InferModelNameLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedInferModelNameLogicalOperator{.operatorId = op.getId(), .modelName = std::make_optional(op->getModelName())});
}

Unreflector<TypedLogicalOperator<InferModelNameLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<InferModelNameLogicalOperator>
Unreflector<TypedLogicalOperator<InferModelNameLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [operatorId, modelName] = context.unreflect<detail::ReflectedInferModelNameLogicalOperator>(rfl);

    if (!modelName.has_value())
    {
        throw CannotDeserialize("Failed to deserialize TypedLogicalOperator<InferModelNameLogicalOperator>");
    }

    auto children = plan->getChildrenFor(operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("InferModelNameLogicalOperator requires exactly one child, but got {}", children.size());
    }

    return TypedLogicalOperator<InferModelNameLogicalOperator>{modelName.value(), std::move(children.at(0))};
}

}

std::size_t std::hash<NES::InferModelNameLogicalOperator>::operator()(const NES::InferModelNameLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getModelName());
}
