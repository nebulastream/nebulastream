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

#include <Operators/InternalFieldCleanupLogicalOperator.hpp>

#include <functional>
#include <ranges>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Traits/Trait.hpp>
#include <Util/Hash.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

InternalFieldCleanupLogicalOperator::InternalFieldCleanupLogicalOperator(
    WeakLogicalOperator self, std::vector<Identifier> internalFields)
    : ManagedByOperator(std::move(self)), internalFields(std::move(internalFields))
{
}

InternalFieldCleanupLogicalOperator::InternalFieldCleanupLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, std::vector<Identifier> internalFields)
    : ManagedByOperator(std::move(self)), child(std::move(child)), internalFields(std::move(internalFields))
{
    inferLocalSchema();
}

TypedLogicalOperator<InternalFieldCleanupLogicalOperator>
InternalFieldCleanupLogicalOperator::create(std::vector<Identifier> internalFields)
{
    return TypedLogicalOperator<InternalFieldCleanupLogicalOperator>{std::move(internalFields)};
}

TypedLogicalOperator<InternalFieldCleanupLogicalOperator>
InternalFieldCleanupLogicalOperator::create(LogicalOperator child, std::vector<Identifier> internalFields)
{
    return TypedLogicalOperator<InternalFieldCleanupLogicalOperator>{std::move(child), std::move(internalFields)};
}

void InternalFieldCleanupLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when inferring internal-field cleanup schema");
    const auto fields = child->getOutputSchema()
        | std::views::filter([&](const Field& field) { return !std::ranges::contains(internalFields, field.getLastName()); })
        | std::views::transform([](const Field& field) { return field.unbound(); }) | std::ranges::to<std::vector>();
    auto schema = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(fields);
    INVARIANT(schema.has_value(), "Removing fields from a collision-free schema introduced a collision");
    outputSchema = std::move(schema).value();
}

const std::vector<Identifier>& InternalFieldCleanupLogicalOperator::getInternalFields() const
{
    return internalFields;
}

LogicalOperator InternalFieldCleanupLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Internal-field cleanup child is not set");
    return child.value();
}

bool InternalFieldCleanupLogicalOperator::operator==(const InternalFieldCleanupLogicalOperator& rhs) const
{
    return internalFields == rhs.internalFields && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

InternalFieldCleanupLogicalOperator InternalFieldCleanupLogicalOperator::withTraitSet(TraitSet newTraitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(newTraitSet);
    return copy;
}

TraitSet InternalFieldCleanupLogicalOperator::getTraitSet() const
{
    return traitSet;
}

Schema<Field, Unordered> InternalFieldCleanupLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed internal-field cleanup schema before inference");
    return bindToOperator(self.lock(), outputSchema.value());
}

Schema<Field, Ordered> InternalFieldCleanupLogicalOperator::getOrderedOutputSchema(const ChildOutputOrderProvider orderProvider) const
{
    PRECONDITION(child.has_value(), "Internal-field cleanup child is not set");
    return orderProvider(child.value())
        | std::views::filter([&](const Field& field) { return !std::ranges::contains(internalFields, field.getLastName()); })
        | std::views::transform([](const Field& field) { return field.unbound(); }) | RangeBinder{self.lock()}
        | std::ranges::to<Schema<Field, Ordered>>();
}

InternalFieldCleanupLogicalOperator
InternalFieldCleanupLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Internal-field cleanup requires exactly one child");
    auto copy = *this;
    copy.child = std::move(children.front());
    return copy;
}

InternalFieldCleanupLogicalOperator
InternalFieldCleanupLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = withChildrenUnsafe(std::move(children));
    copy.inferLocalSchema();
    return copy;
}

std::vector<LogicalOperator> InternalFieldCleanupLogicalOperator::getChildren() const
{
    return child.has_value() ? std::vector{child.value()} : std::vector<LogicalOperator>{};
}

std::string InternalFieldCleanupLogicalOperator::explain(const ExplainVerbosity, const OperatorId) const
{
    return fmt::format("InternalFieldCleanup({})", internalFields);
}

std::string_view InternalFieldCleanupLogicalOperator::getName() noexcept
{
    return NAME;
}

InternalFieldCleanupLogicalOperator InternalFieldCleanupLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Internal-field cleanup child is not set");
    auto copy = *this;
    copy.child = child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

Reflected Reflector<TypedLogicalOperator<InternalFieldCleanupLogicalOperator>>::operator()(
    const TypedLogicalOperator<InternalFieldCleanupLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedInternalFieldCleanupLogicalOperator{
        .operatorId = op.getId(), .internalFields = op->getInternalFields()});
}

Unreflector<TypedLogicalOperator<InternalFieldCleanupLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<InternalFieldCleanupLogicalOperator>
Unreflector<TypedLogicalOperator<InternalFieldCleanupLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    const auto data = context.unreflect<detail::ReflectedInternalFieldCleanupLogicalOperator>(reflected);
    const auto children = plan->getChildrenFor(data.operatorId, context);
    PRECONDITION(children.size() == 1, "Reflected internal-field cleanup requires exactly one child");
    return InternalFieldCleanupLogicalOperator::create(children.front(), data.internalFields);
}

}

std::size_t std::hash<NES::InternalFieldCleanupLogicalOperator>::operator()(
    const NES::InternalFieldCleanupLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine_generic(NES::Hash{}, op.internalFields);
}
