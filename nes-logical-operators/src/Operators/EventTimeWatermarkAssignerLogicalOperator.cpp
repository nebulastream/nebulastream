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
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/Hash.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(
    LogicalFunction onField, const Windowing::TimeUnit& unit)
    : unit(unit), onField(std::move(onField))
{
}

EventTimeWatermarkAssignerLogicalOperator::EventTimeWatermarkAssignerLogicalOperator(
    LogicalOperator child, DescriptorConfig::Config config)
    : unit(0)
{
    const auto timeVariant = config[ConfigParameters::TIME_MS];
    const auto functionVariant = config[ConfigParameters::FUNCTION];

    if (std::holds_alternative<uint64_t>(timeVariant) and std::holds_alternative<FunctionList>(functionVariant))
    {
        this->child = std::move(child);

        const auto functions = std::get<FunctionList>(functionVariant).functions();
        const auto time = Windowing::TimeUnit(std::get<uint64_t>(timeVariant));

        if (functions.size() != 1)
        {
            throw CannotDeserialize("Expected exactly one function");
        }
        this->onField = FunctionSerializationUtil::deserializeFunction(functions[0], this->child->getOutputSchema());
        this->unit = time;

        this->outputSchema = unbind(this->child->getOutputSchema());
    }
    else
    {
        throw CannotDeserialize("EventTimeWatermarkAssignerLogicalOperator: Unknown configuration variant");
    }
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
    return onField == rhs.onField && unit == rhs.unit && getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet();
}

EventTimeWatermarkAssignerLogicalOperator EventTimeWatermarkAssignerLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    const auto& inputSchema = copy.child->getOutputSchema();
    copy.onField = onField.withInferredDataType(inputSchema);
    copy.outputSchema = unbind(inputSchema);
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

Schema EventTimeWatermarkAssignerLogicalOperator::getOutputSchema() const
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

void EventTimeWatermarkAssignerLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    *funcList.add_functions() = onField.serialize();
    const DescriptorConfig::ConfigType funcVariant = std::move(funcList);
    (*serializableOperator.mutable_config())[ConfigParameters::FUNCTION] = descriptorConfigTypeToProto(funcVariant);

    const DescriptorConfig::ConfigType timeVariant = unit.getMillisecondsConversionMultiplier();
    (*serializableOperator.mutable_config())[ConfigParameters::TIME_MS] = descriptorConfigTypeToProto(timeVariant);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterEventTimeWatermarkAssignerLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize(
            "Expected one child for EventTimeWatermarkAssignerLogicalOperator, but found {}", arguments.children.size());
    }
    return TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator>{
        std::move(arguments.children.at(0)), std::move(arguments.config)};
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