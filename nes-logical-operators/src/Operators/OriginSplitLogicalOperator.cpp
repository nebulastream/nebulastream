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

#include <Operators/OriginSplitLogicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

OriginSplitLogicalOperator::OriginSplitLogicalOperator(WeakLogicalOperator self, LogicalOperator child)
    : ManagedByOperator(std::move(self)), child(std::move(child))
{
    inferLocalSchema();
}

TypedLogicalOperator<OriginSplitLogicalOperator> OriginSplitLogicalOperator::create(LogicalOperator child)
{
    return TypedLogicalOperator<OriginSplitLogicalOperator>{std::move(child)};
}

std::string_view OriginSplitLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool OriginSplitLogicalOperator::operator==(const OriginSplitLogicalOperator& rhs) const
{
    /// The ids the branch stamps live in its trait set, so comparing the traits compares the identities as well.
    return outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

std::string OriginSplitLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ORIGINSPLIT(opId: {}, traitSet: {})", opId, traitSet.explain(verbosity));
    }
    return "ORIGINSPLIT()";
}

void OriginSplitLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    /// The branch forwards the records of its child unchanged; only the origin id they carry differs.
    outputSchema = unbind(child->getOutputSchema());
}

OriginSplitLogicalOperator OriginSplitLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet OriginSplitLogicalOperator::getTraitSet() const
{
    return traitSet;
}

OriginSplitLogicalOperator OriginSplitLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

OriginSplitLogicalOperator OriginSplitLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for an origin split, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

OriginSplitLogicalOperator OriginSplitLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for an origin split, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> OriginSplitLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> OriginSplitLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator OriginSplitLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<OriginSplitLogicalOperator>>::operator()(
    const TypedLogicalOperator<OriginSplitLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedOriginSplitLogicalOperator{.operatorId = op.getId()});
}

Unreflector<TypedLogicalOperator<OriginSplitLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<OriginSplitLogicalOperator>
Unreflector<TypedLogicalOperator<OriginSplitLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id] = context.unreflect<detail::ReflectedOriginSplitLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("OriginSplitLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return OriginSplitLogicalOperator::create(children.at(0));
}
}

uint64_t std::hash<NES::OriginSplitLogicalOperator>::operator()(const NES::OriginSplitLogicalOperator& op) const noexcept
{
    /// The operator itself holds no state; the ids the branch stamps are part of its trait set.
    return std::hash<std::string_view>{}(op.getName());
}
