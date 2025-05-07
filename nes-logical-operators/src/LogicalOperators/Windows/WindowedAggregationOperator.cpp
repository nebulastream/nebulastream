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
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <LogicalFunctions/FieldAccessFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <LogicalOperators/Operator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <OperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Configurations/Descriptor.hpp>
#include <LogicalOperators/Windows/WindowedAggregationOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>

namespace NES::Logical
{

WindowedAggregationOperator::WindowedAggregationOperator(
    std::vector<FieldAccessFunction> onKey,
    std::vector<std::shared_ptr<WindowAggregationFunction>> windowAggregation,
    std::shared_ptr<Windowing::WindowType> windowType)
    : windowAggregation(std::move(windowAggregation)), windowType(std::move(windowType)), onKey(std::move(onKey))
{
}

std::string_view WindowedAggregationOperator::getName() const noexcept
{
    return NAME;
}

std::string WindowedAggregationOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        auto& windowType = getWindowType();
        auto windowAggregation = getWindowAggregation();
        return fmt::format(
            "WINDOW AGGREGATION({}, {}, window type: {})",
            id,
            fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->toString(); }), ", "),
            windowType.toString());
    }
    auto windowAggregation = getWindowAggregation();
    return fmt::format(
        "WINDOW AGG({})",
        fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->getName(); }), ", "));
}

bool WindowedAggregationOperator::operator==(const OperatorConcept& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const WindowedAggregationOperator*>(&rhs))
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

        return *windowType == rhsOperator->getWindowType() &&
               getOutputSchema() == rhsOperator->getOutputSchema() &&
               getInputSchemas() == rhsOperator->getInputSchemas() &&
               getInputOriginIds() == rhsOperator->getInputOriginIds() &&
               getOutputOriginIds() == rhsOperator->getOutputOriginIds();
    }
    return false;
}

Operator WindowedAggregationOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "WindowAggregation should have at least one input");
    
    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas) {
        if (schema != firstSchema) {
            throw CannotInferSchema("All input schemas must be equal for WindowAggregation operator");
        }
    }

    // Infer type of aggregation.
    std::vector<std::shared_ptr<WindowAggregationFunction>> newFunctions;
    for (const auto& agg : getWindowAggregation())
    {
        agg->inferStamp(firstSchema);
        newFunctions.push_back(agg);
    }
    copy.windowAggregation = newFunctions;

    copy.windowType->inferStamp(firstSchema);
    copy.inputSchema = firstSchema;
    copy.outputSchema.clear();

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(&getWindowType()))
    {
        const auto& sourceName = firstSchema.getQualifierNameForSystemGeneratedFields();
        const auto& newQualifierForSystemField = sourceName;

        copy.windowStartFieldName = newQualifierForSystemField + "$start";
        copy.windowEndFieldName = newQualifierForSystemField + "$end";
        copy.outputSchema.addField(copy.windowStartFieldName, BasicType::UINT64);
        copy.outputSchema.addField(copy.windowEndFieldName, BasicType::UINT64);
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType().toString());
    }

    if (isKeyed())
    {
        auto keys = getKeys();
        auto newKeys = std::vector<FieldAccessFunction>();
        for (auto& key : keys)
        {
            auto newKey = key.withInferredStamp(firstSchema).get<FieldAccessFunction>();
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

Optimizer::TraitSet WindowedAggregationOperator::getTraitSet() const
{
    return {originIdTrait};
}

Operator WindowedAggregationOperator::withChildren(std::vector<Operator> children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::vector<Schema> WindowedAggregationOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema WindowedAggregationOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<std::vector<OriginId>> WindowedAggregationOperator::getInputOriginIds() const
{
    return {inputOriginIds};
}

std::vector<OriginId> WindowedAggregationOperator::getOutputOriginIds() const
{
    return outputOriginIds;
}

Operator WindowedAggregationOperator::withInputOriginIds(std::vector<std::vector<OriginId>> ids) const
{
    PRECONDITION(ids.size() == 1, "Windowed aggregation should have only one input");
    auto copy = *this;
    copy.inputOriginIds = ids[0];
    return copy;
}

Operator WindowedAggregationOperator::withOutputOriginIds(std::vector<OriginId> ids) const
{
    PRECONDITION(ids.size() == 1, "Windowed aggregation should have only one output OriginId");
    auto copy = *this;
    copy.outputOriginIds = ids;
    return copy;
}

std::vector<Operator> WindowedAggregationOperator::getChildren() const
{
    return children;
}

bool WindowedAggregationOperator::isKeyed() const
{
    return !onKey.empty();
}

std::vector<std::shared_ptr<WindowAggregationFunction>> WindowedAggregationOperator::getWindowAggregation() const
{
    return windowAggregation;
}

void WindowedAggregationOperator::setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationFunction>> wa)
{
    windowAggregation = std::move(wa);
}

Windowing::WindowType& WindowedAggregationOperator::getWindowType() const
{
    return *windowType;
}

void WindowedAggregationOperator::setWindowType(std::unique_ptr<Windowing::WindowType> wt)
{
    windowType = std::move(wt);
}

std::vector<FieldAccessFunction> WindowedAggregationOperator::getKeys() const
{
    return onKey;
}

[[nodiscard]] std::string WindowedAggregationOperator::getWindowStartFieldName() const
{
    return windowStartFieldName;
}

[[nodiscard]] std::string WindowedAggregationOperator::getWindowEndFieldName() const
{
    return windowEndFieldName;
}

SerializableOperator WindowedAggregationOperator::serialize() const
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

    /// Serialize window aggregations
    AggregationFunctionList aggList;
    for (const auto& agg : getWindowAggregation()) {
        *aggList.add_functions() = agg->serialize();
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_AGGREGATIONS] =
        Configurations::descriptorConfigTypeToProto(aggList);

    /// Serialize keys if present
    if (isKeyed()) {
        FunctionList keyList;
        for (const auto& key : getKeys()) {
            *keyList.add_functions() = key.serialize();
        }
        (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_KEYS] =
            Configurations::descriptorConfigTypeToProto(keyList);
    }

    /// Serialize window info
    WindowInfos windowInfo;
    if (auto timeBasedWindow = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType)) {
        auto timeChar = timeBasedWindow->getTimeCharacteristic();
        auto timeCharProto = WindowInfos_TimeCharacteristic();
        timeCharProto.set_type(WindowInfos_TimeCharacteristic_Type_EventTime);
        if(timeChar.field)
        {
            timeCharProto.set_field(timeChar.field->getName());
        }
        timeCharProto.set_multiplier(timeChar.getTimeUnit().getMillisecondsConversionMultiplier());
        windowInfo.mutable_time_characteristic()->CopyFrom(timeCharProto);
        if (auto tumblingWindow = std::dynamic_pointer_cast<Windowing::TumblingWindow>(windowType)) {
            auto* tumbling = windowInfo.mutable_tumbling_window();
            tumbling->set_size(tumblingWindow->getSize().getTime());
        } else if (auto slidingWindow = std::dynamic_pointer_cast<Windowing::SlidingWindow>(windowType)) {
            auto* sliding = windowInfo.mutable_sliding_window();
            sliding->set_size(slidingWindow->getSize().getTime());
            sliding->set_slide(slidingWindow->getSlide().getTime());
        }
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_INFOS] = Configurations::descriptorConfigTypeToProto(windowInfo);

    // Serialize window field names
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_START_FIELD_NAME] =
        Configurations::descriptorConfigTypeToProto(windowStartFieldName);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME] =
        Configurations::descriptorConfigTypeToProto(windowEndFieldName);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
    return serializableOperator;
}

OperatorRegistryReturnType
OperatorGeneratedRegistrar::RegisterWindowedAggregationOperator(OperatorRegistryArguments arguments)
{
    auto aggregationsVariant = arguments.config[WindowedAggregationOperator::ConfigParameters::WINDOW_AGGREGATIONS];
    auto keysVariant = arguments.config[WindowedAggregationOperator::ConfigParameters::WINDOW_KEYS];
    auto windowInfoVariant = arguments.config[WindowedAggregationOperator::ConfigParameters::WINDOW_INFOS];
    auto windowStartVariant = arguments.config[WindowedAggregationOperator::ConfigParameters::WINDOW_START_FIELD_NAME];
    auto windowEndVariant = arguments.config[WindowedAggregationOperator::ConfigParameters::WINDOW_END_FIELD_NAME];

    // Get window aggregations
    if (!std::holds_alternative<AggregationFunctionList>(aggregationsVariant)) {
        throw UnknownOperator();
    }
    auto aggregations = std::get<AggregationFunctionList>(aggregationsVariant).functions();
    std::vector<std::shared_ptr<WindowAggregationFunction>> windowAggregations;
    for (const auto& agg : aggregations) {
        auto function = FunctionSerializationUtil::deserializeWindowAggregationFunction(agg);
        windowAggregations.push_back(function);
    }

    // Get keys if present
    std::vector<FieldAccessFunction> keys;
    if (std::holds_alternative<FunctionList>(keysVariant)) {
        auto keyFunctions = std::get<FunctionList>(keysVariant).functions();
        for (const auto& key : keyFunctions) {
            auto function = FunctionSerializationUtil::deserializeFunction(key);
            if (auto fieldAccess = function.tryGet<FieldAccessFunction>()) {
                keys.push_back(fieldAccess.value());
            } else {
                throw UnknownOperator();
            }
        }
    }

    std::shared_ptr<Windowing::WindowType> windowType;
    if (std::holds_alternative<WindowInfos>(windowInfoVariant)) {
        auto windowInfoProto = std::get<WindowInfos>(windowInfoVariant);
        if (windowInfoProto.has_tumbling_window()) {
            if (windowInfoProto.time_characteristic().type() == WindowInfos_TimeCharacteristic_Type_IngestionTime)
            {
                auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
                windowType = std::make_shared<Windowing::TumblingWindow>(timeChar,
                                                        Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            } else
            {
                auto field = FieldAccessFunction(windowInfoProto.time_characteristic().field());
                auto multiplier = windowInfoProto.time_characteristic().multiplier();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier));
                windowType = std::make_shared<Windowing::TumblingWindow>(timeChar,
                                                        Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            }

        } else if (windowInfoProto.has_sliding_window()) {
            if (windowInfoProto.time_characteristic().type() == WindowInfos_TimeCharacteristic_Type_IngestionTime)
            {
                auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
                windowType = Windowing::SlidingWindow::of(timeChar,
                                                                       Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                                                                       Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            } else
            {
                auto field = FieldAccessFunction(windowInfoProto.time_characteristic().field());
                auto multiplier = windowInfoProto.time_characteristic().multiplier();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier));
                windowType = Windowing::SlidingWindow::of(timeChar,
                                                                       Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                                                                       Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            }
        }
    }
    if (!windowType) {
        throw UnknownOperator();
    }

    auto logicalOperator = WindowedAggregationOperator(keys, windowAggregations, windowType);
    if (auto& id = arguments.id) {
        logicalOperator.id = *id;
    }
    return logicalOperator.withInferredSchema(arguments.inputSchemas)
        .withInputOriginIds(arguments.inputOriginIds)
        .withOutputOriginIds(arguments.outputOriginIds);}

}
