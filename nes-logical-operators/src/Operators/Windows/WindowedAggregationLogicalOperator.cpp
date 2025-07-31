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

#include <fmt/format.h>
#include <fmt/ranges.h>
#include "Serialization/IdentifierSerializationUtil.hpp"
#include "WindowTypes/Measures/TimeCharacteristic.hpp"

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Serialization/TimeCharacteristicSerializationUtil.hpp>
#include <Serialization/WindowTypeSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

namespace
{

Schema inferSchema(const WindowedAggregationLogicalOperator& forOperator)
{
    std::vector<Field> outputFields{};

    outputFields.push_back(forOperator.getWindowStartField());
    outputFields.push_back(forOperator.getWindowEndField());

    for (const auto& [aggFunction, name] : forOperator.getWindowAggregation())
    {
        outputFields.emplace_back(forOperator, name, aggFunction->getAggregateType());
    }
    const auto fieldsAreBound = std::holds_alternative<std::vector<std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>>>(
        forOperator.getGroupingKeysWithName());
    PRECONDITION(fieldsAreBound, "Internal schema inference of windowed aggregation called before binding grouping keys");
    for (const auto& [aggFunction, name] :
         std::get<std::vector<std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>>>(forOperator.getGroupingKeysWithName()))
    {
        if (name.has_value())
        {
            outputFields.emplace_back(forOperator, name.value(), aggFunction.getDataType());
        }
    }

    const auto outputSchemaOrCollisions = Schema::tryCreateCollisionFree(outputFields);
    if (not outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema("Found collisions in inpu schema: " + Schema::createCollisionString(outputSchemaOrCollisions.error()));
    }

    return outputSchemaOrCollisions.value();
}
}

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    GroupingKeyType groupingKey,
    std::vector<ProjectedAggregation> aggregationFunctions,
    std::shared_ptr<Windowing::WindowType> windowType,
    Windowing::TimeCharacteristic timeCharacteristic)
    : windowType(std::move(windowType))
    , groupingKey(std::move(groupingKey))
    , aggregationFunctions(std::move(aggregationFunctions))
    , timestampField(std::move(timeCharacteristic))
{
}

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(LogicalOperator child, DescriptorConfig::Config config)
{
    auto aggregationsVariant = config[ConfigParameters::WINDOW_AGGREGATIONS];
    auto keysVariant = config[ConfigParameters::WINDOW_KEYS];
    auto windowTypeVariant = config[ConfigParameters::WINDOW_TYPE];
    auto timeCharacteristicVariant = config[ConfigParameters::TIME_CHARACTERISTIC];
    auto windowStartVariant = config[ConfigParameters::WINDOW_START_FIELD_NAME];
    auto windowEndVariant = config[ConfigParameters::WINDOW_END_FIELD_NAME];

    if (std::holds_alternative<AggregationFunctionList>(aggregationsVariant) && std::holds_alternative<FunctionList>(keysVariant)
        && std::holds_alternative<SerializableWindowType>(windowTypeVariant)
        && std::holds_alternative<SerializableTimeCharacteristic>(timeCharacteristicVariant)
        && std::holds_alternative<std::string>(windowStartVariant) && std::holds_alternative<std::string>(windowEndVariant))
    {
        this->child = std::move(child);
        const Schema& inputSchema = child.getOutputSchema();
        std::vector<Field> outputFields{};

        auto aggregations = std::get<AggregationFunctionList>(aggregationsVariant).functions();
        std::vector<ProjectedAggregation> windowAggregations;
        for (const auto& agg : aggregations)
        {
            auto function = FunctionSerializationUtil::deserializeWindowAggregationFunction(agg.function(), inputSchema);
            windowAggregations.emplace_back(function, IdentifierSerializationUtil::deserializeIdentifier(agg.projection_name()));
            outputFields.emplace_back(
                Field{*this, IdentifierSerializationUtil::deserializeIdentifier(agg.projection_name()), function->getAggregateType()});
        }
        this->aggregationFunctions = windowAggregations;

        std::vector<std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>> keys;
        for (const auto& pair : std::get<ProjectionList>(keysVariant).projections())
        {
            std::optional<Identifier> projectedName = [&] -> std::optional<Identifier>
            {
                if (pair.has_identifier())
                {
                    return IdentifierSerializationUtil::deserializeIdentifier(pair.identifier());
                }
                return std::nullopt;
            }();

            auto deserializedFunction = FunctionSerializationUtil::deserializeFunction(pair.function(), inputSchema);
            auto fieldAccessOpt = deserializedFunction.tryGet<FieldAccessLogicalFunction>();
            if (!fieldAccessOpt.has_value())
            {
                throw CannotDeserialize("Expected field access function, got {}", deserializedFunction.getType());
            }
            keys.emplace_back(std::move(fieldAccessOpt).value(), std::move(projectedName));
        }
        this->groupingKey = keys;

        const auto serializedWindowType = std::get<SerializableWindowType>(windowTypeVariant);
        this->windowType = WindowTypeSerializationUtil::deserializeWindowType(serializedWindowType);
        if (!windowType)
        {
            throw CannotDeserialize("Unknown window type");
        }
        const auto deserializedCharacteristics1Opt = TimeCharacteristicSerializationUtil::deserializeBoundCharacteristic(
            std::get<SerializableTimeCharacteristic>(timeCharacteristicVariant), inputSchema);

        if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(getWindowType().get()))
        {
            windowMetaData = WindowMetaData{
                Field{*this, Identifier{"start"}, DataType::Type::UINT64}, Field{*this, Identifier{"end"}, DataType::Type::UINT64}};
        }
        else
        {
            throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
        }

        outputSchema = inferSchema(*this);
    }
    throw CannotDeserialize("Invalid variants in WindowedAggregationLogicalOperator config");
}

std::string_view WindowedAggregationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string WindowedAggregationLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    std::variant<std::vector<std::string>, std::vector<uint64_t>> myVariant{std::vector<std::string>{}};
    if (verbosity == ExplainVerbosity::Debug)
    {
        auto windowType = getWindowType();
        auto windowAggregation = getWindowAggregation();
        return fmt::format(
            "WINDOW AGGREGATION(opId: {}, {}, window type: {})",
            id,
            fmt::join(
                std::views::transform(
                    windowAggregation, [](const auto& agg) { return fmt::format("{} as {}", agg.name, agg.function->toString()); }),
                ", "),
            windowType->toString());
    }
    auto windowAggregation = getWindowAggregation();
    return fmt::format(
        "WINDOW AGG({})",
        fmt::join(
            std::views::transform(
                windowAggregation, [](const auto& agg) { return fmt::format("{} as {}", agg.name, agg.function->toString()); }),
            ", "));
}

bool WindowedAggregationLogicalOperator::operator==(const WindowedAggregationLogicalOperator& rhs) const
{
    if (this->isKeyed() != rhs.isKeyed())
    {
        return false;
    }

    if (!std::visit(
            [](const auto& thisGroupingKey, const auto& rhsGroupingKeys)
            {
                if (thisGroupingKey.size() != rhsGroupingKeys.size())
                {
                    return false;
                }

                for (uint64_t i = 0; i < thisGroupingKey.size(); i++)
                {
                    if (thisGroupingKey[i].second != rhsGroupingKeys[i].second
                        || !thisGroupingKey[i].first.operator==(rhsGroupingKeys[i].first))
                    {
                        return false;
                    }
                }
                return true;
            },
            groupingKey,
            rhs.groupingKey))
    {
        return false;
    }

    const auto rhsWindowAggregation = rhs.getWindowAggregation();
    if (aggregationFunctions.size() != rhsWindowAggregation.size())
    {
        return false;
    }
    for (uint64_t i = 0; i < aggregationFunctions.size(); i++)
    {
        if ((aggregationFunctions[i]) != (rhsWindowAggregation[i]))
        {
            return false;
        }
    }

    return *windowType == *rhs.windowType && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

WindowedAggregationLogicalOperator WindowedAggregationLogicalOperator::withInferredSchema() const
{
    auto copy = *this;
    copy.child = copy.child.withInferredSchema();
    const Schema& inputSchema = copy.child.getOutputSchema();

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(getWindowType().get()))
    {
        copy.windowMetaData = WindowMetaData{
            Field{copy, Identifier{"start"}, DataType::Type::UINT64}, Field{copy, Identifier{"end"}, DataType::Type::UINT64}};
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
    }

    if (isKeyed())
    {
        copy.groupingKey = std::visit(
            [&inputSchema](const auto& visitingGroupingKey)
            {
                return visitingGroupingKey
                    | std::views::transform(
                           [&](const auto& key)
                           {
                               return std::make_pair(
                                   FieldAccessLogicalFunctionVariantWrapper{key.first}.withInferredDataType(inputSchema), key.second);
                           })
                    | std::ranges::to<std::vector>();
            },
            groupingKey);
    }

    std::vector<ProjectedAggregation> newFunctions;
    for (const auto& [function, identifier] : aggregationFunctions)
    {
        auto inferredFunction = function->withInferredType(inputSchema);
        newFunctions.emplace_back(inferredFunction, identifier);
    }
    copy.aggregationFunctions = newFunctions;

    copy.timestampField = Windowing::TimeCharacteristicWrapper{std::move(copy.timestampField)}.withInferredSchema(inputSchema);
    copy.outputSchema = inferSchema(copy);
    return copy;
}

TraitSet WindowedAggregationLogicalOperator::getTraitSet() const
{
    return traitSet;
}

WindowedAggregationLogicalOperator WindowedAggregationLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

WindowedAggregationLogicalOperator WindowedAggregationLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for windowedAggregation, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

Schema WindowedAggregationLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Retrieving output schema before calling schema inference");
    return outputSchema.value();
}

std::vector<LogicalOperator> WindowedAggregationLogicalOperator::getChildren() const
{
    return {child};
}

LogicalOperator WindowedAggregationLogicalOperator::getChild() const
{
    return child;
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !std::visit([](const auto& keys) { return keys.empty(); }, groupingKey);
}

std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return aggregationFunctions;
}

std::shared_ptr<Windowing::WindowType> WindowedAggregationLogicalOperator::getWindowType() const
{
    return windowType;
}

Util::VariantContainer<std::vector, UnboundFieldAccessLogicalFunction, FieldAccessLogicalFunction>
WindowedAggregationLogicalOperator::getGroupingKeys() const
{
    return std::visit(
        [](const auto& keys)
        {
            return Util::VariantContainer<std::vector, UnboundFieldAccessLogicalFunction, FieldAccessLogicalFunction>{
                keys | std::views::transform([](const auto& key) { return key.first; }) | std::ranges::to<std::vector>()};
        },
        groupingKey);
}

const WindowedAggregationLogicalOperator::GroupingKeyType& WindowedAggregationLogicalOperator::getGroupingKeysWithName() const
{
    return groupingKey;
}

Field WindowedAggregationLogicalOperator::getWindowStartField() const
{
    INVARIANT(windowMetaData.has_value(), "Retrieving window start field before calling schema inference");
    return windowMetaData->startField;
}

Field WindowedAggregationLogicalOperator::getWindowEndField() const
{
    INVARIANT(windowMetaData.has_value(), "Retrieving window end field before calling schema inference");
    return windowMetaData->endField;
}

const WindowMetaData& WindowedAggregationLogicalOperator::getWindowMetaData() const
{
    INVARIANT(windowMetaData.has_value(), "Retrieving window metadata before calling schema inference");
    return windowMetaData.value();
}

std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>
WindowedAggregationLogicalOperator::getCharacteristic() const
{
    return timestampField;
}

void WindowedAggregationLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    /// Serialize window aggregations
    AggregationFunctionList aggList;
    for (const auto& [agg, name] : getWindowAggregation())
    {
        auto projection = *aggList.add_functions();
        *projection.mutable_function() = agg->serialize();
        IdentifierSerializationUtil::serializeIdentifier(name, projection.mutable_projection_name());
    }
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_AGGREGATIONS] = descriptorConfigTypeToProto(aggList);

    /// Serialize keys if present
    if (isKeyed())
    {
        FunctionList keyList;
        std::visit(
            [&keyList](const auto& keys)
            {
                for (const auto& key : keys)
                {
                    *keyList.add_functions() = key.serialize();
                }
            },
            getGroupingKeys());
        (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_KEYS] = descriptorConfigTypeToProto(keyList);
    }

    /// Serialize window type
    SerializableWindowType windowType;
    WindowTypeSerializationUtil::serializeWindowType(this->windowType, &windowType);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_TYPE] = descriptorConfigTypeToProto(windowType);

    SerializableTimeCharacteristic timeCharacteristic;
    TimeCharacteristicSerializationUtil::serializeCharacteristic(Windowing::TimeCharacteristic{this->timestampField}, &timeCharacteristic);
    (*serializableOperator.mutable_config())[ConfigParameters::TIME_CHARACTERISTIC] = descriptorConfigTypeToProto(timeCharacteristic);

    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_START_FIELD_NAME]
        = descriptorConfigTypeToProto(windowMetaData->startField.getLastName());
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME]
        = descriptorConfigTypeToProto(windowMetaData->endField.getLastName());

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterWindowedAggregationLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Expected one child for WindowedAggregationLogicalOperator, but found {}", arguments.children.size());
    }
    return WindowedAggregationLogicalOperator(std::move(arguments.children.at(0)), std::move(arguments.config));
}

}
