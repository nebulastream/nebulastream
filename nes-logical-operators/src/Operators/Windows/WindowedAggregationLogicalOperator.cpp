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
#include <folly/Hash.h>
#include "Iterators/BFSIterator.hpp"
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
#include <Util/Hash.hpp>
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

auto inferSchema(
    const UnboundFieldBase<1>& windowStartField,
    const UnboundFieldBase<1>& windowEndField,
    const std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation>& aggregationFunctions,
    const WindowedAggregationLogicalOperator::GroupingKeyType& groupingKeys) -> SchemaBase<UnboundFieldBase<1>, false>
{
    std::vector<UnboundFieldBase<1>> outputFields{};

    outputFields.push_back(windowStartField);
    outputFields.push_back(windowEndField);

    for (const auto& [aggFunction, name] : aggregationFunctions)
    {
        outputFields.emplace_back(name, aggFunction->getAggregateType());
    }
    const auto fieldsAreBound
        = std::holds_alternative<std::vector<std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>>>(groupingKeys);
    PRECONDITION(fieldsAreBound, "Internal schema inference of windowed aggregation called before binding grouping keys");
    for (const auto& [aggFunction, name] :
         std::get<std::vector<std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>>>(groupingKeys))
    {
        if (name.has_value())
        {
            outputFields.emplace_back(name.value(), aggFunction.getDataType());
        }
    }

    const auto outputSchemaOrCollisions = SchemaBase<UnboundFieldBase<1>, false>::tryCreateCollisionFree(outputFields);
    if (not outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in inpu schema: " + SchemaBase<UnboundFieldBase<1>, false>::createCollisionString(outputSchemaOrCollisions.error()));
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
    , groupingKeys(std::move(groupingKey))
    , aggregationFunctions(std::move(aggregationFunctions))
    , timestampField(std::move(timeCharacteristic))
{
}

WindowedAggregationLogicalOperator::WindowedAggregationLogicalOperator(
    LogicalOperator child, DescriptorConfig::Config config)
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
        this->groupingKeys = keys;

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
            startEndField = std::array{
                UnboundFieldBase{IdentifierList::create(Identifier::parse("start")), DataType::Type::UINT64},
                UnboundFieldBase{IdentifierList::create(Identifier::parse("end")), DataType::Type::UINT64}};
        }
        else
        {
            throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
        }

        outputSchema = inferSchema((*startEndField)[0], (*startEndField)[1], aggregationFunctions, groupingKeys);
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
            groupingKeys,
            rhs.groupingKeys))
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
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    const Schema& inputSchema = copy.child->getOutputSchema();

    if (auto* timeWindow = dynamic_cast<Windowing::TimeBasedWindowType*>(getWindowType().get()))
    {
        static constexpr auto startId = Identifier::parse("start");
        static constexpr auto endId = Identifier::parse("end");
        copy.startEndField = std::array{
            UnboundFieldBase{IdentifierList::create(startId), DataType::Type::UINT64},
            UnboundFieldBase{IdentifierList::create(endId), DataType::Type::UINT64}};
    }
    else
    {
        throw CannotInferSchema("Unsupported window type {}", getWindowType()->toString());
    }

    if (isKeyed())
    {
        copy.groupingKeys = std::visit(
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
            groupingKeys);
    }

    std::vector<ProjectedAggregation> newFunctions;
    for (const auto& [function, identifier] : aggregationFunctions)
    {
        auto inferredFunction = function->withInferredType(inputSchema);
        newFunctions.emplace_back(inferredFunction, identifier);
    }
    copy.aggregationFunctions = newFunctions;

    copy.timestampField = Windowing::TimeCharacteristicWrapper{std::move(copy.timestampField)}.withInferredSchema(inputSchema);
    copy.outputSchema = inferSchema((*copy.startEndField)[0], (*copy.startEndField)[1], copy.aggregationFunctions, copy.groupingKeys);
    return copy;
}

const detail::DynamicBase* WindowedAggregationLogicalOperator::getDynamicBase() const
{
    return static_cast<const Reprojecter*>(this);
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
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> WindowedAggregationLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator WindowedAggregationLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

bool WindowedAggregationLogicalOperator::isKeyed() const
{
    return !std::visit([](const auto& keys) { return keys.empty(); }, groupingKeys);
}

std::vector<WindowedAggregationLogicalOperator::ProjectedAggregation> WindowedAggregationLogicalOperator::getWindowAggregation() const
{
    return aggregationFunctions;
}

std::shared_ptr<Windowing::WindowType> WindowedAggregationLogicalOperator::getWindowType() const
{
    return windowType;
}

VariantContainer<std::vector, UnboundFieldAccessLogicalFunction, FieldAccessLogicalFunction>
WindowedAggregationLogicalOperator::getGroupingKeys() const
{
    return std::visit(
        [](const auto& keys)
        {
            return VariantContainer<std::vector, UnboundFieldAccessLogicalFunction, FieldAccessLogicalFunction>{
                keys | std::views::transform([](const auto& key) { return key.first; }) | std::ranges::to<std::vector>()};
        },
        groupingKeys);
}

std::unordered_map<Field, std::unordered_set<Field>> WindowedAggregationLogicalOperator::getAccessedFieldsForOutput() const
{
    const auto isBound
        = std::holds_alternative<std::vector<std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>>>(groupingKeys);
    INVARIANT(isBound, "Tried accessing accessed fields for output before calling schema inference");
    auto boundGroupingKeys = std::get<std::vector<std::pair<FieldAccessLogicalFunction, std::optional<Identifier>>>>(groupingKeys);

    const auto outputSchema = getOutputSchema();

    std::unordered_map<Field, std::unordered_set<Field>> accessedFields;
    for (const auto& [aggFunction, name] : getWindowAggregation())
    {
        const auto writeFieldOpt = outputSchema.getFieldByName(name);
        INVARIANT(writeFieldOpt.has_value(), "Field {} not found in output schema", name);
        for (const auto& function : BFSRange{aggFunction->getInputFunction()})
        {
            if (const auto& fieldAccessOpt = function.tryGet<FieldAccessLogicalFunction>())
            {
                accessedFields[writeFieldOpt.value()].insert(fieldAccessOpt.value().getField());
            }
        }
    }
    return accessedFields;
}

const WindowedAggregationLogicalOperator::GroupingKeyType& WindowedAggregationLogicalOperator::getGroupingKeysWithName() const
{
    return groupingKeys;
}

const UnboundFieldBase<1>& WindowedAggregationLogicalOperator::getWindowStartField() const
{
    INVARIANT(startEndField.has_value(), "Retrieving window start field before calling schema inference");
    return startEndField.value()[0];
}

const UnboundFieldBase<1>& WindowedAggregationLogicalOperator::getWindowEndField() const
{
    INVARIANT(startEndField.has_value(), "Retrieving window end field before calling schema inference");
    return startEndField.value()[1];
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
        = descriptorConfigTypeToProto(*startEndField.value()[0].getFullyQualifiedName().begin());
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME]
        = descriptorConfigTypeToProto(*startEndField.value()[1].getFullyQualifiedName().begin());

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterWindowedAggregationLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Expected one child for WindowedAggregationLogicalOperator, but found {}", arguments.children.size());
    }
    return TypedLogicalOperator<WindowedAggregationLogicalOperator>{std::move(arguments.children.at(0)), std::move(arguments.config)};
}

bool operator==(
    const WindowedAggregationLogicalOperator::ProjectedAggregation& lhs,
    const WindowedAggregationLogicalOperator::ProjectedAggregation& rhs)
{
    return *lhs.function == rhs.function && lhs.name == rhs.name;
}
}

std::size_t std::hash<NES::WindowedAggregationLogicalOperator>::operator()(
    const NES::WindowedAggregationLogicalOperator& windowedAggregationLogicalOperator) const noexcept
{
    return folly::hash::hash_combine(
        *windowedAggregationLogicalOperator.windowType,
        windowedAggregationLogicalOperator.aggregationFunctions,
        windowedAggregationLogicalOperator.groupingKeys,
        windowedAggregationLogicalOperator.timestampField,
        windowedAggregationLogicalOperator.startEndField);
}

std::size_t std::hash<NES::WindowedAggregationLogicalOperator::ProjectedAggregation>::operator()(
    const NES::WindowedAggregationLogicalOperator::ProjectedAggregation& aggregation) const noexcept
{
    return folly::hash::hash_combine(*aggregation.function, aggregation.name);
}
