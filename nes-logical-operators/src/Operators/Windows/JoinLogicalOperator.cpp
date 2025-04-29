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
#include <variant>
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Functions/FieldAssignmentLogicalFunction.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <SerializableFunction.pb.h>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>

namespace NES
{

JoinLogicalOperator::JoinLogicalOperator(
    LogicalFunction joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    JoinType joinType)
    : joinFunction(joinFunction)
    , windowType(std::move(windowType))
    , joinType(joinType)
{
}

std::string_view JoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool JoinLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const JoinLogicalOperator*>(&rhs))
    {
        return *getWindowType() == *rhsOperator->getWindowType()
            and getJoinFunction() == rhsOperator->getJoinFunction()
            and getOutputSchema() == rhsOperator->outputSchema
            and getRightSchema() == rhsOperator->getRightSchema()
            and getLeftSchema() == rhsOperator->getLeftSchema();
    }
    return false;
}

std::string JoinLogicalOperator::toString() const
{
    auto result = fmt::format("Join({}, windowType = {}, joinFunction = {})", id, getWindowType()->toString(), getJoinFunction());

    if (!inputOriginIds.empty() || !outputOriginIds.empty())
    {
        result.append(", originIds = {");

        if (inputOriginIds.size() == 2)
        {
            result.append("left: [");
            bool first = true;
            for (const auto& oid : inputOriginIds[1])
            {
                if (!first)
                {
                    result.append(", ");
                }
                result.append(oid.toString());
                first = false;
            }
            result.append("], ");

            result.append("right: [");
            first = true;
            for (const auto& oid : inputOriginIds[0])
            {
                if (!first)
                {
                    result.append(", ");
                }
                result.append(oid.toString());
                first = false;
            }
            result.append("]");
        }

        if (!outputOriginIds.empty())
        {
            if (inputOriginIds.size() == 2)
            {
                result.append(", ");
            }
            result.append("output: [");
            bool first = true;
            for (const auto& oid : outputOriginIds)
            {
                if (!first)
                {
                    result.append(", ");
                }
                result.append(oid.toString());
                first = false;
            }
            result.append("]");
        }
        result.append("}");
    }

    result.append(")");
    return result;
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

    copy.windowStartFieldName = newQualifierForSystemField + "$start";
    copy.windowEndFieldName = newQualifierForSystemField + "$end";
    copy.outputSchema.addField(copy.windowStartFieldName, BasicType::UINT64);
    copy.outputSchema.addField(copy.windowEndFieldName, BasicType::UINT64);

    for (auto field : leftInputSchema)
    {
        copy.outputSchema.addField(field.getName(), field.getDataType());
    }
    for (auto field : rightInputSchema)
    {
        copy.outputSchema.addField(field.getName(), field.getDataType());
    }

    auto inputSchema = leftInputSchema;
    auto combinedSchema = inputSchema.copyFields(rightInputSchema);
    copy.joinFunction = joinFunction.withInferredStamp(combinedSchema);
    copy.windowType->inferStamp(combinedSchema);
    return copy;
}

Optimizer::TraitSet JoinLogicalOperator::getTraitSet() const
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
    return windowStartFieldName;
}

std::string JoinLogicalOperator::getWindowEndFieldName() const
{
    return windowEndFieldName;
}

LogicalFunction JoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

SerializableOperator JoinLogicalOperator::serialize() const
{
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
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    SerializableOperator serializableOperator;
    serializableOperator.set_operator_id(id.getRawValue());
    for (const auto& child : getChildren()) {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

   WindowInfos windowInfo;
    if (auto timeBasedWindow = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType)) {
        auto timeChar = timeBasedWindow->getTimeCharacteristic();
        auto* timeCharProto = new WindowInfos_TimeCharacteristic();
        timeCharProto->set_type(WindowInfos_TimeCharacteristic_Type_EventTime);
        if(timeChar.field)
        {
            timeCharProto->set_field(timeChar.field->getName());
        }
        timeCharProto->set_multiplier(timeChar.getTimeUnit().getMillisecondsConversionMultiplier());

        if (auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType)) {
            auto* tumbling = windowInfo.mutable_tumbling_window();
            tumbling->set_size(tumblingWindow->getSize().getTime());
            tumbling->set_allocated_time_characteristic(timeCharProto);
        } else if (auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType)) {
            auto* sliding = windowInfo.mutable_sliding_window();
            sliding->set_size(slidingWindow->getSize().getTime());
            sliding->set_slide(slidingWindow->getSlide().getTime());
            sliding->set_allocated_time_characteristic(timeCharProto);
        }
    }

    FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getJoinFunction().serialize());
    NES::Configurations::DescriptorConfig::ConfigType functionList = list;

    (*serializableOperator.mutable_config())[ConfigParameters::JOIN_FUNCTION] = Configurations::descriptorConfigTypeToProto(functionList);
    (*serializableOperator.mutable_config())[ConfigParameters::JOIN_TYPE] = Configurations::descriptorConfigTypeToProto(Configurations::EnumWrapper(joinType));
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_INFOS] = Configurations::descriptorConfigTypeToProto(windowInfo);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_START_FIELD_NAME] = Configurations::descriptorConfigTypeToProto(windowStartFieldName);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME] = Configurations::descriptorConfigTypeToProto(windowEndFieldName);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType LogicalOperatorGeneratedRegistrar::RegisterJoinLogicalOperator(NES::LogicalOperatorRegistryArguments arguments)
{
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
        if (std::holds_alternative<WindowInfos>(windowInfoVariant)) {
            auto windowInfoProto = std::get<WindowInfos>(windowInfoVariant);
            if (windowInfoProto.has_tumbling_window()) {
                auto timeCharProto = windowInfoProto.tumbling_window().time_characteristic();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(
                    FieldAccessLogicalFunction(timeCharProto.field()),
                    Windowing::TimeUnit(timeCharProto.multiplier()));
                windowType = std::make_shared<Windowing::TumblingWindow>(timeChar,
                                                                        Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            } else if (windowInfoProto.has_sliding_window()) {
                auto timeCharProto = windowInfoProto.sliding_window().time_characteristic();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(
                    FieldAccessLogicalFunction(timeCharProto.field()),
                    Windowing::TimeUnit(timeCharProto.multiplier()));
                windowType = Windowing::SlidingWindow::of(timeChar,
                                                       Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                                                       Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            }
        }
        if (!windowType) {
            throw UnknownLogicalOperator();
        }

        auto logicalOperator = JoinLogicalOperator(function, windowType, joinType.asEnum<JoinLogicalOperator::JoinType>().value());
        if (auto& id = arguments.id) {
            logicalOperator.id = *id;
        }
        return logicalOperator.withInferredSchema(arguments.inputSchemas)
            .withInputOriginIds(arguments.inputOriginIds)
            .withOutputOriginIds(arguments.outputOriginIds);
    }
    throw UnknownLogicalOperator();
}

}
