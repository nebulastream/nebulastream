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

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <API/Schema.hpp>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES
{

JoinLogicalOperator::JoinLogicalOperator(LogicalFunction joinFunction, std::shared_ptr<Windowing::WindowType> windowType, JoinType joinType)
    : joinFunction(std::move(joinFunction)), windowType(std::move(windowType)), joinType(joinType)
{
}

std::string_view JoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool JoinLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const JoinLogicalOperator*>(&rhs))
    {
        return *getWindowType() == *rhsOperator->getWindowType() and getJoinFunction() == rhsOperator->getJoinFunction()
            and getOutputSchema() == rhsOperator->outputSchema and getRightSchema() == rhsOperator->getRightSchema()
            and getLeftSchema() == rhsOperator->getLeftSchema() and getInputOriginIds() == rhsOperator->getInputOriginIds()
            and getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
}

std::string JoinLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "Join({}-{}, windowType = {}, joinFunction = {}, windowStartField={}, windowEndField={})",
            id,
            fmt::join(outputOriginIds, ", "),
            getWindowType()->toString(),
            getJoinFunction().explain(verbosity),
            windowMetaData.windowStartFieldName,
            windowMetaData.windowEndFieldName);
    }
    return fmt::format("Join({})", getJoinFunction().explain(verbosity));
}

LogicalOperator JoinLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    const auto& leftInputSchema = inputSchemas[0];
    const auto& rightInputSchema = inputSchemas[1];

    auto copy = *this;
    copy.outputSchema.clear();
    copy.leftInputSchema = leftInputSchema;
    copy.rightInputSchema = rightInputSchema;

    auto sourceNameLeft = leftInputSchema.getQualifierNameForSystemGeneratedFields();
    auto sourceNameRight = rightInputSchema.getQualifierNameForSystemGeneratedFields();
    auto newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    copy.windowMetaData.windowStartFieldName = newQualifierForSystemField + "$start";
    copy.windowMetaData.windowEndFieldName = newQualifierForSystemField + "$end";
    copy.outputSchema.addField(copy.windowMetaData.windowStartFieldName, BasicType::UINT64);
    copy.outputSchema.addField(copy.windowMetaData.windowEndFieldName, BasicType::UINT64);

    for (const auto& field : leftInputSchema)
    {
        copy.outputSchema.addField(field.getName(), field.getDataType());
    }
    for (const auto& field : rightInputSchema)
    {
        copy.outputSchema.addField(field.getName(), field.getDataType());
    }

    auto inputSchema = leftInputSchema;
    auto combinedSchema = inputSchema.copyFields(rightInputSchema);
    copy.joinFunction = joinFunction.withInferredDataType(combinedSchema);
    copy.windowType->inferStamp(combinedSchema);
    return copy;
}

TraitSet JoinLogicalOperator::getTraitSet() const
{
    return {originIdTrait};
}

LogicalOperator JoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> JoinLogicalOperator::getInputSchemas() const
{
    return {leftInputSchema, rightInputSchema};
};

Schema JoinLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> JoinLogicalOperator::getInputOriginIds() const
{
    return inputOriginIds;
}

std::vector<OriginId> JoinLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator JoinLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 2, "Join should have only two inputs");
    auto copy = *this;
    copy.inputOriginIds = ids;
    return copy;
}

LogicalOperator JoinLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> JoinLogicalOperator::getChildren() const
{
    return children;
}

Schema JoinLogicalOperator::getLeftSchema() const
{
    return leftInputSchema;
}

Schema JoinLogicalOperator::getRightSchema() const
{
    return rightInputSchema;
}

std::shared_ptr<Windowing::WindowType> JoinLogicalOperator::getWindowType() const
{
    return windowType;
}

std::string JoinLogicalOperator::getWindowStartFieldName() const
{
    return windowMetaData.windowStartFieldName;
}

std::string JoinLogicalOperator::getWindowEndFieldName() const
{
    return windowMetaData.windowEndFieldName;
}

const WindowMetaData& JoinLogicalOperator::getWindowMetaData() const
{
    return windowMetaData;
}

LogicalFunction JoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

SerializableOperator JoinLogicalOperator::serialize() const
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
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    WindowInfos windowInfo;
    if (auto timeBasedWindow = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType))
    {
        auto timeChar = timeBasedWindow->getTimeCharacteristic();
        auto timeCharProto = WindowInfos_TimeCharacteristic();
        timeCharProto.set_type(WindowInfos_TimeCharacteristic_Type_Event_time);
        if (timeChar.field)
        {
            timeCharProto.set_field(timeChar.field->getName());
        }
        timeCharProto.set_multiplier(timeChar.getTimeUnit().getMillisecondsConversionMultiplier());
        windowInfo.mutable_time_characteristic()->CopyFrom(timeCharProto);
        if (auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType))
        {
            auto* tumbling = windowInfo.mutable_tumbling_window();
            tumbling->set_size(tumblingWindow->getSize().getTime());
        }
        else if (auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType))
        {
            auto* sliding = windowInfo.mutable_sliding_window();
            sliding->set_size(slidingWindow->getSize().getTime());
            sliding->set_slide(slidingWindow->getSlide().getTime());
        }
    }

    FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getJoinFunction().serialize());
    const NES::Configurations::DescriptorConfig::ConfigType functionList = list;

    (*serializableOperator.mutable_config())[ConfigParameters::JOIN_FUNCTION] = Configurations::descriptorConfigTypeToProto(functionList);
    (*serializableOperator.mutable_config())[ConfigParameters::JOIN_TYPE]
        = Configurations::descriptorConfigTypeToProto(Configurations::EnumWrapper(joinType));
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_INFOS] = Configurations::descriptorConfigTypeToProto(windowInfo);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_START_FIELD_NAME]
        = Configurations::descriptorConfigTypeToProto(windowMetaData.windowStartFieldName);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME]
        = Configurations::descriptorConfigTypeToProto(windowMetaData.windowEndFieldName);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterJoinLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
    PRECONDITION(arguments.inputSchemas.size() == 2, "Expected two input schemas, but got {}", arguments.inputSchemas.size());

    auto functionVariant = arguments.config[JoinLogicalOperator::ConfigParameters::JOIN_FUNCTION];
    auto joinTypeVariant = arguments.config[JoinLogicalOperator::ConfigParameters::JOIN_TYPE];
    auto windowInfoVariant = arguments.config[JoinLogicalOperator::ConfigParameters::WINDOW_INFOS];
    auto windowStartVariant = arguments.config[JoinLogicalOperator::ConfigParameters::WINDOW_START_FIELD_NAME];
    auto windowEndVariant = arguments.config[JoinLogicalOperator::ConfigParameters::WINDOW_END_FIELD_NAME];

    if (std::holds_alternative<NES::FunctionList>(functionVariant) and std::holds_alternative<Configurations::EnumWrapper>(joinTypeVariant)
        and std::holds_alternative<std::string>(windowStartVariant) and std::holds_alternative<std::string>(windowEndVariant))
    {
        auto functions = std::get<FunctionList>(functionVariant).functions();
        auto joinType = std::get<Configurations::EnumWrapper>(joinTypeVariant);
        auto windowStartFieldName = std::get<std::string>(windowStartVariant);
        auto windowEndFieldName = std::get<std::string>(windowEndVariant);

        INVARIANT(functions.size() == 1, "Expected exactly one function");
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);

        std::shared_ptr<Windowing::WindowType> windowType;
        if (std::holds_alternative<WindowInfos>(windowInfoVariant))
        {
            auto windowInfoProto = std::get<WindowInfos>(windowInfoVariant);
            const auto& timeCharProto = windowInfoProto.time_characteristic();
            if (windowInfoProto.has_tumbling_window())
            {
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(
                    FieldAccessLogicalFunction(timeCharProto.field()), Windowing::TimeUnit(timeCharProto.multiplier()));
                windowType = std::make_shared<Windowing::TumblingWindow>(
                    timeChar, Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            }
            else if (windowInfoProto.has_sliding_window())
            {
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(
                    FieldAccessLogicalFunction(timeCharProto.field()), Windowing::TimeUnit(timeCharProto.multiplier()));
                windowType = Windowing::SlidingWindow::of(
                    timeChar,
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            }
        }
        if (!windowType)
        {
            throw UnknownLogicalOperator();
        }

        auto logicalOperator = JoinLogicalOperator(function, windowType, joinType.asEnum<JoinLogicalOperator::JoinType>().value());
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
