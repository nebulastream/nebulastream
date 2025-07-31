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

#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Schema/Binder.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Field.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator(WeakLogicalOperator self)
    : self(std::move(self))
{
}

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child)
    : self(std::move(self)), child(std::move(child))
{
    inferLocalSchema();
}

std::string_view IngestionTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string IngestionTimeWatermarkAssignerLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INGESTIONTIMEWATERMARKASSIGNER(opId: {}, traitSet: {})", id, traitSet.explain(verbosity));
    }
    return "INGESTION_TIME_WATERMARK_ASSIGNER";
}

bool IngestionTimeWatermarkAssignerLogicalOperator::operator==(const IngestionTimeWatermarkAssignerLogicalOperator& rhs) const
{
    return outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

void IngestionTimeWatermarkAssignerLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto inputSchema = child->getOutputSchema();
    outputSchema = unbind(inputSchema);
}

IngestionTimeWatermarkAssignerLogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet IngestionTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return traitSet;
}

IngestionTimeWatermarkAssignerLogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

IngestionTimeWatermarkAssignerLogicalOperator
IngestionTimeWatermarkAssignerLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for ingestionTimeWatermarkAssigner, got {}", children.size());
    auto copy = *this;
    copy.child = children.at(0);
    return copy;
}

Schema<Field, Unordered> IngestionTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> IngestionTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

Reflected Reflector<TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>>::operator()(
    const TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>& op) const
{
    return reflect(op.getId());
}

Unreflector<TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>
Unreflector<TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto children = plan->getChildrenFor(context.unreflect<OperatorId>(reflected), context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("IngestionTimeWatermarkAssignerLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator>{children.at(0)};
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

}

uint64_t std::hash<NES::IngestionTimeWatermarkAssignerLogicalOperator>::operator()(
    const NES::IngestionTimeWatermarkAssignerLogicalOperator&) const noexcept
{
    return 35890319; /// NOLINT(readability-magic-numbers)
}
