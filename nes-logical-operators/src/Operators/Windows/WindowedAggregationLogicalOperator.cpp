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

#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
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

namespace NES
{

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    std::vector<FieldAccessLogicalFunction> groupingKey,
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> aggregationFunctions,
    std::shared_ptr<Windowing::WindowType> windowType)
    : aggregationFunctions(std::move(aggregationFunctions)), windowType(std::move(windowType)), groupingKey(std::move(groupingKey))
{
}

std::string_view WindowedAggregationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string WindowedAggregationLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        auto windowType = getWindowType();
        auto windowAggregation = getWindowAggregation();
        return fmt::format(
            "WINDOW AGGREGATION(opId: {}, {}, window type: {})",
            id,
            fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->toString(); }), ", "),
            windowType->toString());
    }
    auto windowAggregation = getWindowAggregation();
    return fmt::format(
        "WINDOW AGG({})", fmt::join(std::views::transform(windowAggregation, [](const auto& agg) { return agg->getName(); }), ", "));
}

bool WindowedAggregationLogicalOperator::operator==(const LogicalOperatorConcept& rhs) const
{
    if (const auto* const rhsOperator = dynamic_cast<const WindowedAggregationLogicalOperator*>(&rhs))
    {
        if (this->isKeyed() != rhsOperator->isKeyed())
        {
            return false;
        }

        if (this->getGroupingKeys().size() != rhsOperator->getGroupingKeys().size())
        {
            return false;
        }

        for (uint64_t i = 0; i < this->getGroupingKeys().size(); i++)
        {
            if (this->getGroupingKeys()[i] != rhsOperator->getGroupingKeys()[i])
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

        return windowType.get() == rhsOperator->getWindowType().get() && getOutputSchema() == rhsOperator->getOutputSchema()
            && getInputSchemas() == rhsOperator->getInputSchemas() && getInputOriginIds() == rhsOperator->getInputOriginIds()
            && getOutputOriginIds() == rhsOperator->getOutputOriginIds() && getTraitSet() == rhsOperator->getTraitSet();
    }
    return false;
}

LogicalOperator WindowedAggregationLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "WindowAggregation should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for WindowAggregation operator");
        }
    }

    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> newFunctions;
    for (const auto& agg : getWindowAggregation())
    {
        agg->inferStamp(firstSchema);
        newFunctions.push_back(agg);
    }
    copy.aggregationFunctions = newFunctions;

    copy.windowType->inferStamp(firstSchema);
    copy.inputSchema = firstSchema;
    copy.outputSchema = Schema{copy.outputSchema.memoryLayoutType};

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(getWindowType().get()))
    {
        const auto& newQualifierForSystemField = firstSchema.getQualifierNameForSystemGeneratedFieldsWithSeparator();

        copy.windowMetaData.windowStartFieldName = newQualifierForSystemField + "start";
        copy.windowMetaData.windowEndFieldName = newQualifierForSystemField + "end";
        copy.outputSchema.addField(copy.windowMetaData.windowStartFieldName, DataType::Type::UINT64);
        copy.outputSchema.addField(copy.windowMetaData.windowEndFieldName, DataType::Type::UINT64);
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
    }

    if (isKeyed())
    {
        auto keys = getGroupingKeys();
        auto newKeys = std::vector<FieldAccessLogicalFunction>();
        for (auto& key : keys)
        {
            auto newKey = key.withInferredDataType(firstSchema).get<FieldAccessLogicalFunction>();
            newKeys.push_back(newKey);
            copy.outputSchema.addField(newKey.getFieldName(), newKey.getDataType());
        }
        copy.groupingKey = newKeys;
    }
    for (const auto& agg : copy.aggregationFunctions)
    {
        copy.outputSchema.addField(agg->asField.getFieldName(), agg->asField.getDataType());
    }
    return copy;
}

TraitSet WindowedAggregationLogicalOperator::getTraitSet() const
{
    TraitSet result = traitSet;
    result.emplace_back(originIdTrait);
    return result;
}

LogicalOperator WindowedAggregationLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
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
    return !groupingKey.empty();
}

std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return aggregationFunctions;
}

void WindowedAggregationLogicalOperator::setWindowAggregation(std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> wa)
{
    aggregationFunctions = std::move(wa);
}

std::shared_ptr<Windowing::WindowType> WindowedAggregationLogicalOperator::getWindowType() const
{
    return windowType;
}

void WindowedAggregationLogicalOperator::setWindowType(std::shared_ptr<Windowing::WindowType> wt)
{
    windowType = std::move(wt);
}

std::vector<FieldAccessLogicalFunction> WindowedAggregationLogicalOperator::getGroupingKeys() const
{
    return groupingKey;
}

std::string WindowedAggregationLogicalOperator::getWindowStartFieldName() const
{
    return windowMetaData.windowStartFieldName;
}

std::string WindowedAggregationLogicalOperator::getWindowEndFieldName() const
{
    return windowMetaData.windowEndFieldName;
}

const WindowMetaData& WindowedAggregationLogicalOperator::getWindowMetaData() const
{
    return windowMetaData;
}

SerializableOperator WindowedAggregationLogicalOperator::serialize() const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);
    auto* traitSetProto = proto.mutable_trait_set();
    for (const auto& trait : getTraitSet())
    {
        *traitSetProto->add_traits() = trait.serialize();
    }

    const auto inputs = getInputSchemas();
    const auto originLists = getInputOriginIds();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto* inSch = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(inputs[i], inSch);

        auto* olist = proto.add_input_origin_lists();
        for (auto originId : originLists[i])
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

    /// Serialize window aggregations
    AggregationFunctionList aggList;
    for (const auto& agg : getWindowAggregation())
    {
        *aggList.add_functions() = agg->serialize();
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_AGGREGATIONS] = Configurations::descriptorConfigTypeToProto(aggList);

    /// Serialize keys if present
    if (isKeyed())
    {
        FunctionList keyList;
        for (const auto& key : getGroupingKeys())
        {
            *keyList.add_functions() = key.serialize();
        }
        (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_KEYS] = Configurations::descriptorConfigTypeToProto(keyList);
    }

    /// Serialize window info
    WindowInfos windowInfo;
    if (auto timeBasedWindow = std::dynamic_pointer_cast<Windowing::TimeBasedWindowType>(windowType))
    {
        auto timeChar = timeBasedWindow->getTimeCharacteristic();
        auto timeCharProto = WindowInfos_TimeCharacteristic();
        timeCharProto.set_type(WindowInfos_TimeCharacteristic_Type_Event_time);
        timeCharProto.set_field(timeChar.field.name);
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
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_INFOS] = Configurations::descriptorConfigTypeToProto(windowInfo);

    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_START_FIELD_NAME]
        = Configurations::descriptorConfigTypeToProto(windowMetaData.windowStartFieldName);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME]
        = Configurations::descriptorConfigTypeToProto(windowMetaData.windowEndFieldName);

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

    if (!std::holds_alternative<AggregationFunctionList>(aggregationsVariant))
    {
        throw UnknownLogicalOperator();
    }
    auto aggregations = std::get<AggregationFunctionList>(aggregationsVariant).functions();
    std::vector<std::shared_ptr<WindowAggregationLogicalFunction>> windowAggregations;
    for (const auto& agg : aggregations)
    {
        auto function = FunctionSerializationUtil::deserializeWindowAggregationFunction(agg);
        windowAggregations.push_back(function);
    }

    std::vector<FieldAccessLogicalFunction> keys;
    if (std::holds_alternative<FunctionList>(keysVariant))
    {
        auto keyFunctions = std::get<FunctionList>(keysVariant).functions();
        for (const auto& key : keyFunctions)
        {
            auto function = FunctionSerializationUtil::deserializeFunction(key);
            if (auto fieldAccess = function.tryGet<FieldAccessLogicalFunction>())
            {
                keys.push_back(fieldAccess.value());
            }
            else
            {
                throw UnknownLogicalOperator();
            }
        }
    }

    std::shared_ptr<Windowing::WindowType> windowType;
    if (std::holds_alternative<WindowInfos>(windowInfoVariant))
    {
        auto windowInfoProto = std::get<WindowInfos>(windowInfoVariant);
        if (windowInfoProto.has_tumbling_window())
        {
            if (windowInfoProto.time_characteristic().type() == WindowInfos_TimeCharacteristic_Type_Ingestion_time)
            {
                auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
                windowType = std::make_shared<Windowing::TumblingWindow>(
                    timeChar, Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            }
            else
            {
                auto field = FieldAccessLogicalFunction(windowInfoProto.time_characteristic().field());
                auto multiplier = windowInfoProto.time_characteristic().multiplier();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier));
                windowType = std::make_shared<Windowing::TumblingWindow>(
                    timeChar, Windowing::TimeMeasure(windowInfoProto.tumbling_window().size()));
            }
        }
        else if (windowInfoProto.has_sliding_window())
        {
            if (windowInfoProto.time_characteristic().type() == WindowInfos_TimeCharacteristic_Type_Ingestion_time)
            {
                auto timeChar = Windowing::TimeCharacteristic::createIngestionTime();
                windowType = Windowing::SlidingWindow::of(
                    timeChar,
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            }
            else
            {
                auto field = FieldAccessLogicalFunction(windowInfoProto.time_characteristic().field());
                auto multiplier = windowInfoProto.time_characteristic().multiplier();
                auto timeChar = Windowing::TimeCharacteristic::createEventTime(field, Windowing::TimeUnit(multiplier));
                windowType = Windowing::SlidingWindow::of(
                    timeChar,
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().size()),
                    Windowing::TimeMeasure(windowInfoProto.sliding_window().slide()));
            }
        }
    }
    if (!windowType)
    {
        throw UnknownLogicalOperator();
    }

    auto logicalOperator = WindowedAggregationLogicalOperator(keys, windowAggregations, windowType);
    if (auto& id = arguments.id)
    {
        logicalOperator.id = *id;
    }
    return logicalOperator.withInferredSchema(arguments.inputSchemas)
        .withInputOriginIds(arguments.inputOriginIds)
        .withOutputOriginIds(arguments.outputOriginIds);
}

}
