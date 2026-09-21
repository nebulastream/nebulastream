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

#include <Operators/SemanticMapNameLogicalOperator.hpp>

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

SemanticMapNameLogicalOperator::SemanticMapNameLogicalOperator(WeakLogicalOperator self, std::string modelName)
    : ManagedByOperator(std::move(self)), modelName(std::move(modelName))
{
}

SemanticMapNameLogicalOperator::SemanticMapNameLogicalOperator(WeakLogicalOperator self, std::string modelName, LogicalOperator child)
    : ManagedByOperator(std::move(self)), modelName(std::move(modelName)), child(std::move(child))
{
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string_view SemanticMapNameLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string SemanticMapNameLogicalOperator::getModelName() const
{
    return modelName;
}

bool SemanticMapNameLogicalOperator::operator==(const SemanticMapNameLogicalOperator& rhs) const
{
    return modelName == rhs.modelName && getTraitSet() == rhs.getTraitSet();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
std::string SemanticMapNameLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SEM_MAP_NAME(opId: {}, model: {}, traitSet: {})", opId, modelName, traitSet.explain(verbosity));
    }
    return fmt::format("SEM_MAP_NAME(model: {})", modelName);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalOperatorConcept, cannot be static
SemanticMapNameLogicalOperator SemanticMapNameLogicalOperator::withInferredSchema() const
{
    PRECONDITION(false, "SemanticMapName requires model resolution before schema inference");
    std::unreachable();
}

TraitSet SemanticMapNameLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SemanticMapNameLogicalOperator SemanticMapNameLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

SemanticMapNameLogicalOperator SemanticMapNameLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for SemanticMapName, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

/// NOLINTBEGIN(readability-convert-member-functions-to-static, performance-unnecessary-value-param)
SemanticMapNameLogicalOperator SemanticMapNameLogicalOperator::withChildren(std::vector<LogicalOperator>) const
{
    PRECONDITION(false, "SemanticMapName requires model resolution before schema inference");
    std::unreachable();
}

/// NOLINTEND(readability-convert-member-functions-to-static, performance-unnecessary-value-param)

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Schema<Field, Unordered> SemanticMapNameLogicalOperator::getOutputSchema() const
{
    PRECONDITION(false, "SemanticMapName requires model resolution and schema inference before retrieving output schema");
    std::unreachable();
}

Schema<Field, Ordered> SemanticMapNameLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider) const
{
    PRECONDITION(false, "SemanticMapName requires model resolution before ordered schema inference");
    std::unreachable();
}

std::vector<LogicalOperator> SemanticMapNameLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator SemanticMapNameLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<SemanticMapNameLogicalOperator>>::operator()(
    const TypedLogicalOperator<SemanticMapNameLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedSemanticMapNameLogicalOperator{.operatorId = op.getId(), .modelName = std::make_optional(op->getModelName())});
}

Unreflector<TypedLogicalOperator<SemanticMapNameLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<SemanticMapNameLogicalOperator>
Unreflector<TypedLogicalOperator<SemanticMapNameLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [operatorId, modelName] = context.unreflect<detail::ReflectedSemanticMapNameLogicalOperator>(rfl);

    if (!modelName.has_value())
    {
        throw CannotDeserialize("Failed to deserialize TypedLogicalOperator<SemanticMapNameLogicalOperator>");
    }

    auto children = plan->getChildrenFor(operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("SemanticMapNameLogicalOperator requires exactly one child, but got {}", children.size());
    }

    return TypedLogicalOperator<SemanticMapNameLogicalOperator>{modelName.value(), std::move(children.at(0))};
}

}

std::size_t std::hash<NES::SemanticMapNameLogicalOperator>::operator()(const NES::SemanticMapNameLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getModelName());
}
