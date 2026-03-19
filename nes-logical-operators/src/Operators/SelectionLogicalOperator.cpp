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

#include <Operators/SelectionLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{
SelectionLogicalOperator::SelectionLogicalOperator(WeakLogicalOperator self, LogicalFunction predicate)
    : ManagedByOperator(std::move(self)), predicate(std::move(std::move(predicate)))
{
}

SelectionLogicalOperator::SelectionLogicalOperator(WeakLogicalOperator self, LogicalOperator child, LogicalFunction predicate)
    : ManagedByOperator(std::move(self)), child(std::move(child)), predicate(std::move(predicate))
{
    inferLocalSchema();
}

std::string_view SelectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction SelectionLogicalOperator::getPredicate() const
{
    return predicate;
}

bool SelectionLogicalOperator::operator==(const SelectionLogicalOperator& rhs) const
{
    return predicate == rhs.predicate && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
};

std::string SelectionLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SELECTION(opId: {}, predicate: {}, traitSet: {})", opId, predicate.explain(verbosity), traitSet.explain(verbosity));
    }
    return fmt::format("SELECTION({})", predicate.explain(verbosity));
}

void SelectionLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();
    predicate = predicate.withInferredDataType(inputSchema);
    if (not predicate.getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotInferSchema("the selection expression is not a valid predicate");
    }
    outputSchema = unbind(inputSchema);
}

SelectionLogicalOperator SelectionLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet SelectionLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SelectionLogicalOperator SelectionLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

SelectionLogicalOperator SelectionLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for selection, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

Schema<Field, Unordered> SelectionLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> SelectionLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator SelectionLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected
Reflector<TypedLogicalOperator<SelectionLogicalOperator>>::operator()(const TypedLogicalOperator<SelectionLogicalOperator>& op) const
{
    return reflect(detail::ReflectedSelectionLogicalOperator{.operatorId = op.getId(), .predicate = op->getPredicate()});
}

Unreflector<TypedLogicalOperator<SelectionLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<SelectionLogicalOperator>
Unreflector<TypedLogicalOperator<SelectionLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, predicate] = context.unreflect<detail::ReflectedSelectionLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("SelectionLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return TypedLogicalOperator<SelectionLogicalOperator>{children.at(0), predicate};
}
}

uint64_t std::hash<NES::SelectionLogicalOperator>::operator()(const NES::SelectionLogicalOperator& op) const noexcept
{
    return std::hash<NES::LogicalFunction>{}(op.getPredicate());
}
