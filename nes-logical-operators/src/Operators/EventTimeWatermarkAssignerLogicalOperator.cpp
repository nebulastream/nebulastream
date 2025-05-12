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

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

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

std::string EventTimeWatermarkAssignerLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        std::string inputOriginIdsStr;
        if (!inputOriginIds.empty())
        {
            inputOriginIdsStr = fmt::format(", inputOriginIds: [{}]", fmt::join(inputOriginIds, ", "));
        }
        return fmt::format(
            "EVENT_TIME_WATERMARK_ASSIGNER(opId: {}, onField: {}, unit: {}, inputSchema: {}{})",
            id,
            onField.explain(verbosity),
            unit.getMillisecondsConversionMultiplier(),
            inputSchema,
            inputOriginIdsStr);
    }
    return "WATERMARK_ASSIGNER(Event time)";
}

bool EventTimeWatermarkAssignerLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const EventTimeWatermarkAssignerLogicalOperator*>(&rhs))
    {
        return onField == rhsOperator->onField && unit == rhsOperator->unit && getOutputSchema() == rhsOperator->getOutputSchema()
            && getInputSchemas() == rhsOperator->getInputSchemas() && getInputOriginIds() == rhsOperator->getInputOriginIds()
            && getOutputOriginIds() == rhsOperator->getOutputOriginIds() && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
}


LogicalOperator EventTimeWatermarkAssignerLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    PRECONDITION(inputSchemas.size() == 1, "Watermark assigner should have only one input");
    const auto& inputSchema = inputSchemas[0];
    copy.onField = onField.withInferredDataType(inputSchema);
    copy.inputSchema = inputSchema;
    copy.outputSchema = inputSchema;
    return copy;
}

LogicalOperator EventTimeWatermarkAssignerLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}


TraitSet EventTimeWatermarkAssignerLogicalOperator::getTraitSet() const
{
    return traitSet;
}

LogicalOperator EventTimeWatermarkAssignerLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

LogicalOperator EventTimeWatermarkAssignerLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
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

std::vector<std::vector<OriginId>> EventTimeWatermarkAssignerLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> EventTimeWatermarkAssignerLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator EventTimeWatermarkAssignerLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Assigner should have one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}


std::vector<LogicalOperator> EventTimeWatermarkAssignerLogicalOperator::getChildren() const
{
    return children;
}

SerializableOperator EventTimeWatermarkAssignerLogicalOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    for (const auto& inputSchema : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }

    for (const auto& originList : getInputOriginIds())
    {
        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originList)
        {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds())
    {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    *funcList.add_functions() = onField.serialize();
    const Configurations::DescriptorConfig::ConfigType funcVariant = std::move(funcList);
    (*serializableOperator.mutable_config())[ConfigParameters::FUNCTION] = descriptorConfigTypeToProto(funcVariant);

    const Configurations::DescriptorConfig::ConfigType timeVariant = unit.getMillisecondsConversionMultiplier();
    (*serializableOperator.mutable_config())[ConfigParameters::TIME_MS] = descriptorConfigTypeToProto(timeVariant);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterEventTimeWatermarkAssignerLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto timeVariant = arguments.config[EventTimeWatermarkAssignerLogicalOperator::ConfigParameters::TIME_MS];
    auto functionVariant = arguments.config[EventTimeWatermarkAssignerLogicalOperator::ConfigParameters::FUNCTION];

    if (std::holds_alternative<uint64_t>(timeVariant) and std::holds_alternative<FunctionList>(functionVariant))
    {
        const auto functions = std::get<FunctionList>(functionVariant).functions();
        const auto time = Windowing::TimeUnit(std::get<uint64_t>(timeVariant));

        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);

        auto logicalOperator = EventTimeWatermarkAssignerLogicalOperator(function, time);
        if (auto& id = arguments.id)
        {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);
    }
    throw UnknownLogicalOperator();
}
}
