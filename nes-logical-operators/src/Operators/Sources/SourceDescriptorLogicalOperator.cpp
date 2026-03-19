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

#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>

#include <cstddef>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

SourceDescriptorLogicalOperator::SourceDescriptorLogicalOperator(WeakLogicalOperator self, SourceDescriptor sourceDescriptor)
    : ManagedByOperator(std::move(self)), sourceDescriptor(std::move(sourceDescriptor))
{
    inferLocalSchema();
}

std::string_view SourceDescriptorLogicalOperator::getName() const noexcept
{
    return NAME;
}

void SourceDescriptorLogicalOperator::inferLocalSchema()
{
    outputSchema = *sourceDescriptor.getLogicalSource().getSchema() | std::ranges::to<Schema<UnqualifiedUnboundField, Unordered>>();
}

SourceDescriptorLogicalOperator SourceDescriptorLogicalOperator::withInferredSchema() const
{
    auto copy = *this;
    copy.inferLocalSchema();
    return copy;
}

bool SourceDescriptorLogicalOperator::operator==(const SourceDescriptorLogicalOperator& rhs) const
{
    const bool descriptorsEqual = sourceDescriptor == rhs.sourceDescriptor;
    return descriptorsEqual && traitSet == rhs.traitSet;
}

std::string SourceDescriptorLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SOURCE(opId: {}, {}, traitSet: {})", id, sourceDescriptor.explain(verbosity), traitSet.explain(verbosity));
    }
    return fmt::format("SOURCE({})", sourceDescriptor.explain(verbosity));
}

SourceDescriptorLogicalOperator SourceDescriptorLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet SourceDescriptorLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SourceDescriptorLogicalOperator SourceDescriptorLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

Schema<Field, Unordered> SourceDescriptorLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> SourceDescriptorLogicalOperator::getChildren() const
{
    return children;
}

SourceDescriptor SourceDescriptorLogicalOperator::getSourceDescriptor() const
{
    return sourceDescriptor;
}

Schema<Field, Ordered> SourceDescriptorLogicalOperator::getOrderedOutputSchema(ChildOutputOrderProvider) const
{
    return bind(self.lock(), *sourceDescriptor.getLogicalSource().getSchema());
}

const detail::DynamicBase* SourceDescriptorLogicalOperator::getDynamicBase() const
{
    return static_cast<const OriginIdAssigner*>(this);
}

Unreflector<TypedLogicalOperator<SourceDescriptorLogicalOperator>>::Unreflector(ContextType plan) : plan(std::move(plan))
{
}

Reflected Reflector<TypedLogicalOperator<SourceDescriptorLogicalOperator>>::operator()(
    const TypedLogicalOperator<SourceDescriptorLogicalOperator>& op) const
{
    return reflect(
        detail::ReflectedSourceDescriptorLogicalOperator{.operatorId = op.getId(), .sourceDescriptor = op->getSourceDescriptor()});
}

TypedLogicalOperator<SourceDescriptorLogicalOperator>
Unreflector<TypedLogicalOperator<SourceDescriptorLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, sourceDescriptor] = context.unreflect<detail::ReflectedSourceDescriptorLogicalOperator>(rfl);
    if (const auto children = plan->getChildrenFor(id, context); !children.empty())
    {
        throw CannotDeserialize("SourceDescriptorLogicalOperator requires no children, but got {}", children.size());
    }
    return TypedLogicalOperator<SourceDescriptorLogicalOperator>{std::move(sourceDescriptor)};
}

}

std::size_t std::hash<NES::SourceDescriptorLogicalOperator>::operator()(
    const NES::SourceDescriptorLogicalOperator& sourceDescriptorLogicalOperator) const
{
    return std::hash<NES::SourceDescriptor>{}(sourceDescriptorLogicalOperator.getSourceDescriptor());
}
