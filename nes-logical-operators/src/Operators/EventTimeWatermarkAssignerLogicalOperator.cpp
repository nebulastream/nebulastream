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
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/TimeUnit.hpp>
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

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(
    LogicalFunction onField, const Windowing::TimeUnit& unit)
    : onField(std::move(onField)), unit(unit)
{
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
            "EVENT_TIME_WATERMARK_ASSIGNER(opId: {}, onField: {}, unit: {}, inputSchema: {}, traitSet: {})",
            id,
            onField.explain(verbosity),
            unit.getMillisecondsConversionMultiplier(),
            inputSchema,
            traitSet.explain(verbosity));
    }
    return "WATERMARK_ASSIGNER(Event time)";
}

bool EventTimeWatermarkAssignerLogicalOperator::operator==(const EventTimeWatermarkAssignerLogicalOperator& rhs) const
{
    return onField == rhs.onField && unit == rhs.unit && getOutputSchema() == rhs.getOutputSchema()
        && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
}

EventTimeWatermarkAssignerLogicalOperator
EventTimeWatermarkAssignerLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.size() != 1)
    {
        throw CannotDeserialize("Watermark assigner should have only one input");
    }
    const auto& inputSchema = inputSchemas[0];
    copy.onField = onField.withInferredDataType(inputSchema);
    copy.inputSchema = inputSchema;
    copy.outputSchema = inputSchema;
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
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> EventTimeWatermarkAssignerLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema EventTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> EventTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    return children;
}

Reflected Reflector<EventTimeWatermarkAssignerLogicalOperator>::operator()(const EventTimeWatermarkAssignerLogicalOperator& op) const
{
    return reflect(detail::ReflectedEventTimeWatermarkAssignerLogicalOperator{.onField = op.onField, .timeUnit = op.unit});
}

EventTimeWatermarkAssignerLogicalOperator
Unreflector<EventTimeWatermarkAssignerLogicalOperator>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [onField, timeUnit] = context.unreflect<detail::ReflectedEventTimeWatermarkAssignerLogicalOperator>(reflected);
    return EventTimeWatermarkAssignerLogicalOperator{onField, timeUnit};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterEventTimeWatermarkAssignerLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<EventTimeWatermarkAssignerLogicalOperator>(arguments.reflected);
    }

    PRECONDITION(false, "Operator is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}
}
