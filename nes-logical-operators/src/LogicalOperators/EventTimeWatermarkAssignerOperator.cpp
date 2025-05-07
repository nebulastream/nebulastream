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
#include <memory>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <OperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <ErrorHandling.hpp>
#include <FunctionRegistry.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <LogicalOperators/EventTimeWatermarkAssignerOperator.hpp>

namespace NES::Logical
{

EventTimeWatermarkAssignerOperator::EventTimeWatermarkAssignerOperator(Function onField, Windowing::TimeUnit unit)
    : onField(std::move(onField)), unit(unit)
{
}

std::string_view EventTimeWatermarkAssignerOperator::getName() const noexcept
{
    return NAME;
}

std::string EventTimeWatermarkAssignerOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        std::string inputOriginIdsStr;
        if (!inputOriginIds.empty())
        {
            inputOriginIdsStr = fmt::format(", inputOriginIds: [{}]", fmt::join(inputOriginIds, ", "));
        }
        return fmt::format("EVENTTIMEWATERMARKASSIGNER(opId: {}, onField: {}, unit: {}, inputSchema: {}{})",
            id,
            onField.explain(verbosity),
            unit.getMillisecondsConversionMultiplier(),
            inputSchema.toString(),
            inputOriginIdsStr);
    }
    return "WATERMARKASSIGNER(Event time)";
}

bool EventTimeWatermarkAssignerOperator::operator==(const OperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const EventTimeWatermarkAssignerOperator*>(&rhs))
    {
        return onField == rhsOperator->onField &&
               unit == rhsOperator->unit &&
               getOutputSchema() == rhsOperator->getOutputSchema() &&
               getInputSchemas() == rhsOperator->getInputSchemas() &&
               getInputOriginIds() == rhsOperator->getInputOriginIds() &&
               getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
}


Operator EventTimeWatermarkAssignerOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    PRECONDITION(inputSchemas.size() == 1, "Watermark assigner should have only one input");
    const auto& inputSchema = inputSchemas[0];
    copy.onField = onField.withInferredStamp(inputSchema);
    copy.inputSchema = inputSchema;
    copy.outputSchema = inputSchema;
    return copy;
}

Operator EventTimeWatermarkAssignerOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}


Optimizer::TraitSet EventTimeWatermarkAssignerOperator::getTraitSet() const
{
    return {};
}

Operator EventTimeWatermarkAssignerOperator::withChildren(std::vector<Operator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> EventTimeWatermarkAssignerOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema EventTimeWatermarkAssignerOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> EventTimeWatermarkAssignerOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> EventTimeWatermarkAssignerOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

Operator EventTimeWatermarkAssignerOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Assigner should have one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}


std::vector<Operator> EventTimeWatermarkAssignerOperator::getChildren() const
{
    return children;
}

SerializableOperator EventTimeWatermarkAssignerOperator::serialize() const {
    SerializableOperator_LogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (auto const& trait : getTraitSet()) {
        *traitSetProto->add_traits() = trait.serialize();
    }

    for (auto const& inputSchema : getInputSchemas()) {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputSchema, schProto);
    }

    for (auto const& originList : getInputOriginIds()) {
        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originList) {
            olist->add_origin_ids(originId.getRawValue());
        }
    }

    for (auto outId : getOutputOriginIds()) {
        proto.add_output_origin_ids(outId.getRawValue());
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (auto& child : getChildren()) {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    *funcList.add_functions() = onField.serialize();
    Configurations::DescriptorConfig::ConfigType funcVariant = std::move(funcList);
    (*serializableOperator.mutable_config())[ConfigParameters::FUNCTION] = descriptorConfigTypeToProto(funcVariant);

    Configurations::DescriptorConfig::ConfigType timeVariant = unit.getMillisecondsConversionMultiplier();
    (*serializableOperator.mutable_config())[ConfigParameters::TIME_MS] = descriptorConfigTypeToProto(timeVariant);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

OperatorRegistryReturnType
OperatorGeneratedRegistrar::RegisterEventTimeWatermarkAssignerOperator(OperatorRegistryArguments arguments)
{
    auto timeVariant = arguments.config[EventTimeWatermarkAssignerOperator::ConfigParameters::TIME_MS];
    auto functionVariant = arguments.config[EventTimeWatermarkAssignerOperator::ConfigParameters::FUNCTION];

    if (std::holds_alternative<uint64_t>(timeVariant) and std::holds_alternative<FunctionList>(functionVariant)) {
        const auto functions = std::get<FunctionList>(functionVariant).functions();
        const auto time = Windowing::TimeUnit(std::get<uint64_t>(timeVariant));

        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);

        auto logicalOperator = EventTimeWatermarkAssignerOperator(function, time);
        if (auto& id = arguments.id) {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);    }
    throw UnknownOperator();
}
}
