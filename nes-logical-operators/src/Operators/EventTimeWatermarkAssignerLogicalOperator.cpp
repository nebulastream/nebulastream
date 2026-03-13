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

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/Hash.h>
#include "Serialization/ReflectedOperator.hpp"

#include <Configurations/Descriptor.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(
    WeakLogicalOperator self, LogicalFunction onField, const Windowing::TimeUnit& unit)
    : self(std::move(self)), unit(unit), onField(std::move(onField))
{
}

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, LogicalFunction onField, const Windowing::TimeUnit& unit)
    : self(std::move(self)), unit(unit), onField(std::move(onField)), child(std::move(child))
{
    inferLocalSchema();
}

std::string_view EventTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string EventTimeWatermarkAssignerLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "EVENT_TIME_WATERMARK_ASSIGNER(opId: {}, onField: {}, unit: {}, traitSet: {})",
            id,
            onField.explain(verbosity),
            unit.getMillisecondsConversionMultiplier(),
            traitSet.explain(verbosity));
    }
    return "WATERMARK_ASSIGNER(Event time)";
}

bool EventTimeWatermarkAssignerLogicalOperator::operator==(const EventTimeWatermarkAssignerLogicalOperator& rhs) const
{
    return onField == rhs.onField && unit == rhs.unit && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

void EventTimeWatermarkAssignerLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto& inputSchema = child->getOutputSchema();
    onField = onField.withInferredDataType(inputSchema);
    outputSchema = unbind(inputSchema);
}

EventTimeWatermarkAssignerLogicalOperator EventTimeWatermarkAssignerLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet EventTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return traitSet;
}

EventTimeWatermarkAssignerLogicalOperator EventTimeWatermarkAssignerLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

EventTimeWatermarkAssignerLogicalOperator
EventTimeWatermarkAssignerLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for eventTimeWatermarkAssigner, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

Schema<Field, Unordered> EventTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> EventTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalFunction EventTimeWatermarkAssignerLogicalOperator::getOnField() const
{
    return onField;
}

Windowing::TimeUnit EventTimeWatermarkAssignerLogicalOperator::getUnit() const
{
    return unit;
}

Reflected Reflector<TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>>::operator()(
    const TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>& op) const
{
    return reflect(detail::ReflectedEventTimeWatermarkAssignerLogicalOperator{
        .operatorId = op.getId(), .onField = op->getOnField(), .timeUnit = op->getUnit()});
}

Unreflector<TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>
Unreflector<TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, onField, timeUnit] = context.unreflect<detail::ReflectedEventTimeWatermarkAssignerLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("EventTimeWatermarkAssignerLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>{children.at(0), onField, timeUnit};
}

LogicalOperator EventTimeWatermarkAssignerLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}
}

uint64_t std::hash<NES::EventTimeWatermarkAssignerLogicalOperator>::operator()(
    const NES::EventTimeWatermarkAssignerLogicalOperator& eventTimeWatermarkAssignerOperator) const noexcept
{
    return folly::hash::hash_combine(eventTimeWatermarkAssignerOperator.unit, eventTimeWatermarkAssignerOperator.onField);
}
