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
#include <sstream>
#include <string>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Types/ContentBasedWindowType.hpp>
#include <WindowTypes/Types/ThresholdWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Configurations/Descriptor.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>

namespace NES
{

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    std::vector<FieldAccessLogicalFunction> onKey,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregation,
    std::shared_ptr<Windowing::WindowType> windowType)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(onKey)
{
}

std::string_view WindowedAggregationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string WindowedAggregationLogicalOperator::toString() const
{
    std::stringstream ss;
    auto& wt = getWindowType();
    const auto& aggs = getWindowAggregation();
    ss << "WINDOW AGGREGATION(OP-" << id << ", ";
    for (const auto& agg : aggs)
    {
        ss << agg->toString() << ";";
    }
    ss << ") on window type: " << wt.toString();
    return ss.str();
}

bool WindowedAggregationLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const WindowedAggregationLogicalOperator*>(&rhs))
    {
        if (this->isKeyed() != rhsOperator->isKeyed())
        {
            return false;
        }

        if (this->getKeys().size() != rhsOperator->getKeys().size())
        {
            return false;
        }

        for (uint64_t i = 0; i < this->getKeys().size(); i++)
        {
            if (this->getKeys()[i] != rhsOperator->getKeys()[i])
            {
                return false;
            }
        }

        if (this->getWindowAggregation().size() != rhsOperator->getWindowAggregation().size())
        {
            return false;
        }

        for (uint64_t i = 0; i < this->getWindowAggregation().size(); i++)
        {
            if (*(getWindowAggregation()[i]) != (rhsOperator->getWindowAggregation()[i]))
            {
                return false;
            }
        }
        return *windowType == rhsOperator->getWindowType();
    }
    return false;
}

LogicalOperator WindowedAggregationLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    PRECONDITION(inputSchemas.size() == 1, "WindowAggregation should have only one input");
    const auto& inputSchema = inputSchemas[0];

    // Infer type of aggregation.
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> newFunctions;
    for (const auto& agg : getWindowAggregation())
    {
        agg->inferStamp(inputSchema);
        newFunctions.push_back(agg);
    }
    copy.windowAggregation = newFunctions;

    copy.windowType->inferStamp(inputSchema);
    copy.inputSchema = inputSchema;
    copy.outputSchema.clear();

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(&getWindowType()))
    {
        const auto& sourceName = inputSchema.getQualifierNameForSystemGeneratedFields();
        const auto& newQualifierForSystemField = sourceName;

        copy.windowStartFieldName = newQualifierForSystemField + "$start";
        copy.windowEndFieldName = newQualifierForSystemField + "$end";
        copy.outputSchema.addField(copy.windowStartFieldName, BasicType::UINT64);
        copy.outputSchema.addField(copy.windowEndFieldName, BasicType::UINT64);
    }
    else if (auto* contentBasedWindowType = dynamic_cast<Windowing::ContentBasedWindowType*>(&getWindowType()))
    {
        if (contentBasedWindowType->getContentBasedSubWindowType()
            == Windowing::ContentBasedWindowType::ContentBasedSubWindowType::THRESHOLDWINDOW)
        {
            auto thresholdWindow = Windowing::ContentBasedWindowType::asThresholdWindow(*contentBasedWindowType);
        }
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType().toString());
    }

    if (isKeyed())
    {
        auto keys = getKeys();
        auto newKeys = std::vector<FieldAccessLogicalFunction>();
        for (auto& key : keys)
        {
            auto newKey = key.withInferredStamp(inputSchema).get<FieldAccessLogicalFunction>();
            newKeys.push_back(newKey);
            copy.outputSchema.addField(AttributeField(newKey.getFieldName(), newKey.getStamp()));
        }
        copy.onKey = newKeys;
    }
    for (const auto& agg : copy.windowAggregation)
    {
        copy.outputSchema.addField(AttributeField(agg->asField.getFieldName(), agg->asField.getStamp()));
    }
    return copy;
}

Optimizer::TraitSet WindowedAggregationLogicalOperator::getTraitSet() const
{
    return {originIdTrait};
}

LogicalOperator WindowedAggregationLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> WindowedAggregationLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema WindowedAggregationLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> WindowedAggregationLogicalOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> WindowedAggregationLogicalOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

LogicalOperator WindowedAggregationLogicalOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Windowed aggregation should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

LogicalOperator WindowedAggregationLogicalOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    PRECONDITION(ids.size() == 1, "Windowed aggregation should have only one output OriginId");
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<LogicalOperator> WindowedAggregationLogicalOperator::getChildren() const
{
    return children;
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !onKey.empty();
}

std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return windowAggregation;
}

void WindowedAggregationLogicalOperator::setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> wa)
{
    windowAggregation = std::move(wa);
}

Windowing::WindowType& WindowedAggregationLogicalOperator::getWindowType() const
{
    return *windowType;
}

void WindowedAggregationLogicalOperator::setWindowType(std::unique_ptr<Windowing::WindowType> wt)
{
    windowType = std::move(wt);
}

std::vector<FieldAccessLogicalFunction> WindowedAggregationLogicalOperator::getKeys() const
{
    return onKey;
}

[[nodiscard]] std::string WindowedAggregationLogicalOperator::getWindowStartFieldName() const
{
    return windowStartFieldName;
}

[[nodiscard]] std::string WindowedAggregationLogicalOperator::getWindowEndFieldName() const
{
    return windowEndFieldName;
}

SerializableOperator WindowedAggregationLogicalOperator::serialize() const
{
    SerializableOperator_LogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (auto const& trait : getTraitSet()) {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs      = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i) {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i]) {
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

    // Serialize window aggregations
    AggregationFunctionList aggList;
    for (const auto& agg : getWindowAggregation()) {
        *aggList.add_functions() = agg->serialize();
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_AGGREGATIONS] =
        Configurations::descriptorConfigTypeToProto(aggList);

    // Serialize keys if present
    if (isKeyed()) {
        FunctionList keyList;
        for (const auto& key : getKeys()) {
            *keyList.add_functions() = key.serialize();
        }
        (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_KEYS] =
            Configurations::descriptorConfigTypeToProto(keyList);
    }

   // Serialize window info
    WindowInfos windowInfoProto;
    if (auto timeBasedWindow = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType)) {
        if (auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(timeBasedWindow)) {
            auto* tumbling = windowInfoProto.mutable_tumbling_window();
            tumbling->set_size(tumblingWindow->getSize().getTime());
        } else if (auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(timeBasedWindow)) {
            auto* sliding = windowInfoProto.mutable_sliding_window();
            sliding->set_size(slidingWindow->getSize().getTime());
            sliding->set_slide(slidingWindow->getSlide().getTime());
            auto timeChar = slidingWindow->getTimeCharacteristic();
            auto* timeCharProto = new WindowInfos_TimeCharacteristic();
            timeCharProto->set_type(WindowInfos_TimeCharacteristic_Type_EventTime);
            if (timeChar.field)
            {
                timeCharProto->set_field(timeChar.field->getName());
            }
            timeCharProto->set_multiplier(timeChar.getTimeUnit().getMillisecondsConversionMultiplier());
            sliding->set_allocated_time_characteristic(timeCharProto);
        }
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_INFOS] = Configurations::descriptorConfigTypeToProto(windowInfoProto);

    // Serialize window field names
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_START_FIELD_NAME] =
        Configurations::descriptorConfigTypeToProto(windowStartFieldName);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME] =
        Configurations::descriptorConfigTypeToProto(windowEndFieldName);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterWindowedAggregationLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto aggregationsVariant = arguments.config[WindowedAggregationLogicalOperator::ConfigParameters::WINDOW_AGGREGATIONS];
    auto keysVariant = arguments.config[WindowedAggregationLogicalOperator::ConfigParameters::WINDOW_KEYS];
    auto windowInfoVariant = arguments.config[WindowedAggregationLogicalOperator::ConfigParameters::WINDOW_INFOS];
    auto windowStartVariant = arguments.config[WindowedAggregationLogicalOperator::ConfigParameters::WINDOW_START_FIELD_NAME];
    auto windowEndVariant = arguments.config[WindowedAggregationLogicalOperator::ConfigParameters::WINDOW_END_FIELD_NAME];

    // Get window aggregations
    if (!std::holds_alternative<AggregationFunctionList>(aggregationsVariant)) {
        throw UnknownLogicalOperator();
    }
    auto aggregations = std::get<AggregationFunctionList>(aggregationsVariant).functions();
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregations;
    for (const auto& agg : aggregations) {
        auto function = FunctionSerializationUtil::deserializeWindowAggregationFunction(agg);
        windowAggregations.push_back(function);
    }

    // Get keys if present
    std::vector<FieldAccessLogicalFunction> keys;
    if (std::holds_alternative<FunctionList>(keysVariant)) {
        auto keyFunctions = std::get<FunctionList>(keysVariant).functions();
        for (const auto& key : keyFunctions) {
            auto function = FunctionSerializationUtil::deserializeFunction(key);
            if (auto fieldAccess = function.tryGet<FieldAccessLogicalFunction>()) {
                keys.push_back(fieldAccess.value());
            } else {
                throw UnknownLogicalOperator();
            }
        }
    }

    std::shared_ptr<Windowing::WindowType> windowType;
    if (std::holds_alternative<WindowInfos>(windowInfoVariant)) {
        auto windowInfoProto = std::get<WindowInfos>(windowInfoVariant);
        if (windowInfoProto.has_tumbling_window()) {
            auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
            windowType = std::make_shared<Windowing::TumblingWindow>(timeChar,
                                                                    Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
        } else if (windowInfoProto.has_sliding_window()) {
            auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
            windowType = Windowing::SlidingWindow::of(timeChar,
                                                                   Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                                                                   Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
        }
    }
    if (!windowType) {
        throw UnknownLogicalOperator();
    }

    auto logicalOperator = WindowedAggregationLogicalOperator(keys, windowAggregations, windowType);
    if (auto& id = arguments.id) {
        logicalOperator.id = *id;
    }
    return logicalOperator.withInferredSchema(arguments.inputSchemas)
        .withInputOriginIds(arguments.inputOriginIds)
        .withOutputOriginIds(arguments.outputOriginIds);}

}
