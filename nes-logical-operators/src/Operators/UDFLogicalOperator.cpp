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

#include <Operators/UDFLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <UdfDescriptor.hpp>

namespace NES
{

UDFLogicalOperator::UDFLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::vector<NamedCall> calls)
    : ManagedByOperator(std::move(self)), child(std::move(child)), calls(std::move(calls))
{
    PRECONDITION(!this->calls.empty(), "A UDF operator needs at least one call");
    inferLocalSchema();
}

TypedLogicalOperator<UDFLogicalOperator> UDFLogicalOperator::create(LogicalOperator child, std::vector<NamedCall> calls)
{
    return TypedLogicalOperator<UDFLogicalOperator>{std::move(child), std::move(calls)};
}

const std::vector<UDFLogicalOperator::NamedCall>& UDFLogicalOperator::getCalls() const
{
    return calls;
}

UdfExecution UDFLogicalOperator::getExecution() const
{
    const auto call = calls.front().second.tryGetAs<UDFCallLogicalFunction>();
    PRECONDITION(call.has_value() && call.value()->getDescriptor().has_value(), "A UDF operator only holds resolved UDF calls");
    return call.value()->getDescriptor()->getExecution();
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string_view UDFLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool UDFLogicalOperator::operator==(const UDFLogicalOperator& rhs) const
{
    return calls == rhs.calls && getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

std::string UDFLogicalOperator::explain(const ExplainVerbosity verbosity, const OperatorId opId) const
{
    auto explained = calls
        | std::views::transform([&](const NamedCall& call) { return fmt::format("{} = {}", call.first, call.second.explain(verbosity)); });
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "UDF(opId: {}, execution: {}, calls: [{}], traitSet: {})",
            opId,
            magic_enum::enum_name(getExecution()),
            fmt::join(explained, ", "),
            traitSet.explain(verbosity));
    }
    return fmt::format("UDF(execution: {}, calls: [{}])", magic_enum::enum_name(getExecution()), fmt::join(explained, ", "));
}

void UDFLogicalOperator::inferLocalSchema()
{
    const auto childOutput = child.getOutputSchema();
    auto outputFields = childOutput | RangeUnbinder{} | std::ranges::to<std::vector>();
    for (auto& [name, call] : calls)
    {
        call = call.withInferredDataType(childOutput);
        outputFields.emplace_back(name, call.getDataType());
    }
    auto outputSchemaOrCollisions = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(outputFields);
    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "UDF operator output schema has name collisions: "
            + Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    outputSchema = std::move(outputSchemaOrCollisions).value();
}

UDFLogicalOperator UDFLogicalOperator::withInferredSchema() const
{
    auto copy = *this;
    copy.child = copy.child.withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet UDFLogicalOperator::getTraitSet() const
{
    return traitSet;
}

UDFLogicalOperator UDFLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

UDFLogicalOperator UDFLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> newChildren) const
{
    PRECONDITION(newChildren.size() == 1, "Can only set exactly one child for a UDF operator, got {}", newChildren.size());
    auto copy = *this;
    copy.child = std::move(newChildren.front());
    return copy;
}

UDFLogicalOperator UDFLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = withChildrenUnsafe(std::move(newChildren));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> UDFLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

Schema<Field, Ordered> UDFLogicalOperator::getOrderedOutputSchema(const ChildOutputOrderProvider orderProvider) const
{
    /// The child's fields in their order, then the results in the order of the calls.
    std::vector<UnqualifiedUnboundField> fields = orderProvider(child) | RangeUnbinder{} | std::ranges::to<std::vector>();
    for (const auto& [name, call] : calls)
    {
        fields.emplace_back(name, call.getDataType());
    }
    auto orderedOrCollisions = Schema<UnqualifiedUnboundField, Ordered>::tryCreateCollisionFree(std::move(fields));
    if (!orderedOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "UDF operator output schema has name collisions: "
            + Schema<UnqualifiedUnboundField, Ordered>::createCollisionString(orderedOrCollisions.error()));
    }
    return NES::bindToOperator(self.lock(), std::move(orderedOrCollisions).value());
}

std::vector<LogicalOperator> UDFLogicalOperator::getChildren() const
{
    return {child};
}

LogicalOperator UDFLogicalOperator::getChild() const
{
    return child;
}

Reflected Reflector<TypedLogicalOperator<UDFLogicalOperator>>::operator()(
    const TypedLogicalOperator<UDFLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedUDFLogicalOperator{.operatorId = op.getId(), .calls = op->getCalls()});
}

Unreflector<TypedLogicalOperator<UDFLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

TypedLogicalOperator<UDFLogicalOperator>
Unreflector<TypedLogicalOperator<UDFLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [operatorId, calls] = context.unreflect<detail::ReflectedUDFLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(operatorId, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("UDFLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return UDFLogicalOperator::create(children.at(0), std::move(calls));
}

}

std::size_t std::hash<NES::UDFLogicalOperator>::operator()(const NES::UDFLogicalOperator& op) const noexcept
{
    /// NOLINTNEXTLINE(readability-magic-numbers)
    return folly::hash::commutative_hash_combine_range_generic(17, folly::hash::StdHasher{}, op.calls.begin(), op.calls.end());
}
