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

#include <Operators/Windows/JoinLogicalOperator.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/Hash.h>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Serialization/TimeCharacteristicSerializationUtil.hpp>
#include <Serialization/WindowTypeSerializationUtil.hpp>
#include <Traits/ImplementationTypeTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Hash.hpp>
#include <Util/Overloaded.hpp>
#include <Util/PlanRenderer.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>
#include "Schema/Field.hpp"
#include "Serialization/IdentifierSerializationUtil.hpp"
#include "WindowTypes/Measures/TimeCharacteristic.hpp"

namespace NES
{


std::optional<JoinTimeCharacteristic> JoinLogicalOperator::createJoinTimeCharacteristic(
    std::array<std::variant<Windowing::UnboundTimeCharacteristic, Windowing::BoundTimeCharacteristic>, 2> timestampFields)
{
    return std::visit(
        Overloaded{
            [&timestampFields](Windowing::UnboundTimeCharacteristic&& unbound)
            {
                return std::visit(
                    Overloaded{
                        [&unbound](Windowing::UnboundTimeCharacteristic&& unbound2) -> std::optional<JoinTimeCharacteristic>
                        { return std::make_optional<JoinTimeCharacteristic>(std::array{std::move(unbound), std::move(unbound2)}); },
                        [](Windowing::BoundTimeCharacteristic&&) -> std::optional<JoinTimeCharacteristic>
                        { return std::optional<JoinTimeCharacteristic>{}; }},
                    std::move(timestampFields[1]));
            },
            [&timestampFields](Windowing::BoundTimeCharacteristic&& bound)
            {
                return std::visit(
                    Overloaded{
                        [&bound](Windowing::BoundTimeCharacteristic&& bound2) -> std::optional<JoinTimeCharacteristic>
                        { return std::make_optional<JoinTimeCharacteristic>(std::array{std::move(bound), std::move(bound2)}); },
                        [](Windowing::UnboundTimeCharacteristic&&) -> std::optional<JoinTimeCharacteristic>
                        { return std::optional<JoinTimeCharacteristic>{}; }},
                    std::move(timestampFields[1]));
            }},
        std::move(timestampFields[0]));
}

namespace
{
SchemaBase<UnboundFieldBase<1>, false>
inferOutputSchema(const std::array<LogicalOperator, 2>& children, const std::array<UnboundFieldBase<1>, 2>& startEndFields)
{
    const std::vector<Field> inputFields = children | std::views::transform([](const auto& child) { return child.getOutputSchema(); })
        | std::views::join | std::ranges::to<std::vector>();

    std::vector<UnboundFieldBase<1>> outputFields
        = inputFields | std::views::transform([](const Field& field) { return field.unbound(); }) | std::ranges::to<std::vector>();

    outputFields.emplace_back(UnboundFieldBase<1>{startEndFields[0].getFullyQualifiedName(), startEndFields[1].getDataType()});
    outputFields.emplace_back(UnboundFieldBase<1>{startEndFields[1].getFullyQualifiedName(), startEndFields[1].getDataType()});

    auto outputSchemaOrCollisions = SchemaBase<UnboundFieldBase<1>, false>::tryCreateCollisionFree(outputFields);

    if (!outputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema(
            "Found collisions in of input schemas with added fields from windows join: "
            + SchemaBase<UnboundFieldBase<1>, false>::createCollisionString(outputSchemaOrCollisions.error()));
    }
    return outputSchemaOrCollisions.value();
}
}

JoinLogicalOperator::JoinLogicalOperator(
    LogicalFunction joinFunction,
    std::shared_ptr<Windowing::WindowType> windowType,
    JoinType joinType,
    JoinTimeCharacteristic timeCharacteristics)
    : windowType(std::move(windowType))
    , joinType(joinType)
    , joinFunction(std::move(joinFunction))
    , timestampFields(std::move(timeCharacteristics))
{
}

JoinLogicalOperator::JoinLogicalOperator(std::array<LogicalOperator, 2> children, DescriptorConfig::Config config)
{
    auto functionVariant = config[JoinLogicalOperator::ConfigParameters::JOIN_FUNCTION];
    auto joinTypeVariant = config[JoinLogicalOperator::ConfigParameters::JOIN_TYPE];
    auto windowTypeVariant = config[JoinLogicalOperator::ConfigParameters::WINDOW_TYPE];
    auto characteristic1Variant = config[JoinLogicalOperator::ConfigParameters::TIME_CHARACTERISTIC_1];
    auto characteristic2Variant = config[JoinLogicalOperator::ConfigParameters::TIME_CHARACTERISTIC_2];
    auto windowStartVariant = config[JoinLogicalOperator::ConfigParameters::WINDOW_START_FIELD_NAME];
    auto windowEndVariant = config[JoinLogicalOperator::ConfigParameters::WINDOW_END_FIELD_NAME];

    if (std::holds_alternative<NES::FunctionList>(functionVariant) and std::holds_alternative<EnumWrapper>(joinTypeVariant)
        and std::holds_alternative<Identifier>(windowStartVariant) and std::holds_alternative<Identifier>(windowEndVariant)
        and std::holds_alternative<SerializableWindowType>(windowTypeVariant)
        and std::holds_alternative<SerializableTimeCharacteristic>(characteristic1Variant)
        and std::holds_alternative<SerializableTimeCharacteristic>(characteristic2Variant))
    {
        auto functions = std::get<FunctionList>(functionVariant).functions();
        auto joinTypeEnum = std::get<EnumWrapper>(joinTypeVariant);
        auto windowStartFieldName = std::get<Identifier>(windowStartVariant);
        auto windowEndFieldName = std::get<Identifier>(windowEndVariant);

        INVARIANT(functions.size() == 1, "Expected exactly one function");

        this->joinType = joinTypeEnum.asEnum<JoinLogicalOperator::JoinType>().value();

        // Create combined input schema for function deserialization
        const std::vector<Field> inputFields = children | std::views::transform([](const auto& child) { return child.getOutputSchema(); })
            | std::views::join | std::ranges::to<std::vector>();

        auto inputSchemaOrCollisions = Schema::tryCreateCollisionFree(inputFields);

        if (!inputSchemaOrCollisions.has_value())
        {
            throw CannotInferSchema("Found collisions in input schemas: " + Schema::createCollisionString(inputSchemaOrCollisions.error()));
        }
        const auto& inputSchema = inputSchemaOrCollisions.value();

        this->joinFunction = FunctionSerializationUtil::deserializeFunction(functions[0], inputSchemaOrCollisions.value());

        this->startEndFields = std::array{
            UnboundFieldBase{IdentifierList::create(std::move(windowStartFieldName)), DataType::Type::UINT64},
            UnboundFieldBase{IdentifierList::create(std::move(windowEndFieldName)), DataType::Type::UINT64}};

        if (std::holds_alternative<SerializableWindowType>(windowTypeVariant))
        {
            const auto serializedWindowType = std::get<SerializableWindowType>(windowTypeVariant);
            this->windowType = WindowTypeSerializationUtil::deserializeWindowType(serializedWindowType);
        }
        if (!windowType)
        {
            throw CannotDeserialize("Unknown window type");
        }

        const auto deserializedCharacteristics1Opt = TimeCharacteristicSerializationUtil::deserializeBoundCharacteristic(
            std::get<SerializableTimeCharacteristic>(characteristic1Variant), inputSchema);

        const auto deserializedCharacteristics2Opt = TimeCharacteristicSerializationUtil::deserializeBoundCharacteristic(
            std::get<SerializableTimeCharacteristic>(characteristic2Variant), inputSchema);

        if (!deserializedCharacteristics1Opt.has_value() or !deserializedCharacteristics2Opt.has_value())
        {
            throw CannotDeserialize("Could not deserialize time characteristics");
        }
        auto deserializedCharacteristics1 = deserializedCharacteristics1Opt.value();
        auto deserializedCharacteristics2 = deserializedCharacteristics2Opt.value();

        if (std::holds_alternative<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics1)
            && std::holds_alternative<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics2))
        {
            const auto& eventTimeCharacteristic1 = std::get<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics1);
            const auto& eventTimeCharacteristic2 = std::get<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics2);
            if (eventTimeCharacteristic1.field.getField().getProducedBy() == eventTimeCharacteristic2.field.getField().getProducedBy())
            {
                throw CannotDeserialize("EventTimeCharacteristics must point to different input operators");
            }
        }

        /// Ensure that order of time characteristics lines up with order of children
        if ((std::holds_alternative<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics1)
             && std::get<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics1).field.getField().getProducedBy()
                 == children[1])
            || (std::holds_alternative<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics2)
                && std::get<Windowing::BoundEventTimeCharacteristic>(deserializedCharacteristics2).field.getField().getProducedBy()
                    == children[0]))
        {
            std::swap(deserializedCharacteristics1, deserializedCharacteristics2);
        }

        timestampFields = std::array{deserializedCharacteristics1, deserializedCharacteristics2};


        // Infer function and window types
        this->joinFunction = joinFunction.withInferredDataType(inputSchemaOrCollisions.value());

        this->children = std::move(children);
        this->outputSchema = inferOutputSchema(*this->children, this->startEndFields.value());
        return;
    }
    throw UnknownLogicalOperator();
}

std::string_view JoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

bool JoinLogicalOperator::operator==(const JoinLogicalOperator& rhs) const
{
    return *getWindowType() == *rhs.getWindowType() and getJoinFunction() == rhs.getJoinFunction() and outputSchema == rhs.outputSchema
        and getTraitSet() == rhs.getTraitSet();
}

std::string JoinLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "Join(opId: {}, windowType: {}, joinFunction: {}, windowMetadata: (startField: {}, endField: {}), traitSet: {})",
            id,
            getWindowType()->toString(),
            getJoinFunction().explain(verbosity),
            startEndFields.value()[0],
            startEndFields.value()[1],
            traitSet.explain(verbosity));
    }
    return fmt::format("Join({})", getJoinFunction().explain(verbosity));
}

JoinLogicalOperator JoinLogicalOperator::withInferredSchema() const
{
    PRECONDITION(children.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.children = {(*copy.children)[0].withInferredSchema(), (*copy.children)[1].withInferredSchema()};

    const std::vector<Field> inputFields = *copy.children | std::views::transform([](const auto& child) { return child.getOutputSchema(); })
        | std::views::join | std::ranges::to<std::vector>();

    auto inputSchemaOrCollisions = Schema::tryCreateCollisionFree(inputFields);

    if (!inputSchemaOrCollisions.has_value())
    {
        throw CannotInferSchema("Found collisions in input schemas: " + Schema::createCollisionString(inputSchemaOrCollisions.error()));
    }

    /// TODO you could have access to qualified fields in the input schema, but the qualifiers have to be removed in the output schema
    /// But to do this properly you'd need semantic scopes because we do not know what qualifiers are available1
    copy.joinFunction = joinFunction.withInferredDataType(inputSchemaOrCollisions.value());
    copy.timestampFields = std::visit(
        [&inputSchemaOrCollisions](const auto& fields)
        {
            return std::array{
                Windowing::TimeCharacteristicWrapper{fields[0]}.withInferredSchema(inputSchemaOrCollisions.value()),
                Windowing::TimeCharacteristicWrapper{fields[1]}.withInferredSchema(inputSchemaOrCollisions.value())};
        },
        timestampFields);

    static constexpr auto startIdentifier = Identifier::parse("start");
    static constexpr auto endIdentifier = Identifier::parse("end");
    copy.startEndFields = std::array{
        UnboundFieldBase{IdentifierListBase<1>{startIdentifier}, DataType::Type::UINT64},
        UnboundFieldBase{IdentifierListBase<1>{endIdentifier}, DataType::Type::UINT64}};

    copy.outputSchema = inferOutputSchema(*copy.children, copy.startEndFields.value());

    return copy;
}

JoinLogicalOperator JoinLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

TraitSet JoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

JoinLogicalOperator JoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 2, "Can only set exactly two child for join, got {}", children.size());
    auto copy = *this;
    copy.children = std::array{std::move(children.at(0)), std::move(children.at(1))};
    return copy;
}

Schema JoinLogicalOperator::getOutputSchema() const
{
    PRECONDITION(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bind(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> JoinLogicalOperator::getChildren() const
{
    if (children.has_value())
    {
        return *children | std::ranges::to<std::vector>();
    }
    return {};
}

std::array<LogicalOperator, 2> JoinLogicalOperator::getBothChildren() const
{
    PRECONDITION(children.has_value(), "Children not set when trying to retrieve join children");
    return *children;
}

std::shared_ptr<Windowing::WindowType> JoinLogicalOperator::getWindowType() const
{
    return windowType;
}

const UnboundFieldBase<1>& JoinLogicalOperator::getStartField() const
{
    INVARIANT(startEndFields.has_value(), "Retrieving window end field before calling schema inference");
    return startEndFields.value()[0];
}

const UnboundFieldBase<1>& JoinLogicalOperator::getEndField() const
{
    INVARIANT(startEndFields.has_value(), "Retrieving window end field before calling schema inference");
    return startEndFields.value()[1];
}

LogicalFunction JoinLogicalOperator::getJoinFunction() const
{
    return joinFunction;
}

JoinTimeCharacteristic JoinLogicalOperator::getJoinTimeCharacteristics() const
{
    return timestampFields;
}

void JoinLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(getOutputSchema(), outSch);

    for (const auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    SerializableWindowType serializableWindowType;
    WindowTypeSerializationUtil::serializeWindowType(windowType, &serializableWindowType);

    std::visit(
        [&serializableOperator](const auto& characteristics)
        {
            SerializableTimeCharacteristic first;
            SerializableTimeCharacteristic second;
            TimeCharacteristicSerializationUtil::serializeCharacteristic(Windowing::TimeCharacteristic{characteristics[0]}, &first);
            TimeCharacteristicSerializationUtil::serializeCharacteristic(Windowing::TimeCharacteristic{characteristics[1]}, &second);

            (*serializableOperator.mutable_config())[ConfigParameters::TIME_CHARACTERISTIC_1] = descriptorConfigTypeToProto(first);
            (*serializableOperator.mutable_config())[ConfigParameters::TIME_CHARACTERISTIC_2] = descriptorConfigTypeToProto(second);
        },
        timestampFields);

    FunctionList list;
    auto* serializedFunction = list.add_functions();
    serializedFunction->CopyFrom(getJoinFunction().serialize());
    const DescriptorConfig::ConfigType functionList = list;

    (*serializableOperator.mutable_config())[ConfigParameters::JOIN_FUNCTION] = descriptorConfigTypeToProto(functionList);
    (*serializableOperator.mutable_config())[ConfigParameters::JOIN_TYPE] = descriptorConfigTypeToProto(EnumWrapper(joinType));
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_TYPE] = descriptorConfigTypeToProto(serializableWindowType);
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_START_FIELD_NAME]
        = descriptorConfigTypeToProto(*startEndFields.value()[0].getFullyQualifiedName().begin());
    (*serializableOperator.mutable_config())[ConfigParameters::WINDOW_END_FIELD_NAME]
        = descriptorConfigTypeToProto(*startEndFields.value()[1].getFullyQualifiedName().begin());

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType LogicalOperatorGeneratedRegistrar::RegisterJoinLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Expected two children for WindowedAggregationLogicalOperator, but found {}", arguments.children.size());
    }
    return TypedLogicalOperator<JoinLogicalOperator>{
        std::array{std::move(arguments.children.at(0)), std::move(arguments.children.at(1))}, arguments.config};
}

}

std::size_t std::hash<NES::JoinLogicalOperator>::operator()(const NES::JoinLogicalOperator& joinLogicalOperator) const noexcept
{
    return folly::hash::hash_combine(
        joinLogicalOperator.joinType,
        joinLogicalOperator.joinFunction,
        *joinLogicalOperator.windowType,
        joinLogicalOperator.timestampFields,
        joinLogicalOperator.startEndFields);
}

std::size_t std::hash<NES::JoinTimeCharacteristic>::operator()(const NES::JoinTimeCharacteristic& joinTimeCharacteristic) const noexcept
{
    return std::visit(
        [](const auto& characteristic) { return folly::hash::hash_combine(characteristic[0], characteristic[1]); }, joinTimeCharacteristic);
}
