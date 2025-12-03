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

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/BooleanFunctions/AndLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Serialization/WindowTypeReflection.hpp>
#include <Traits/ImplementationTypeTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

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

bool JoinLogicalOperator::operator==(const JoinLogicalOperator& rhs) const
{
    return *getWindowType() == *rhs.getWindowType() and getJoinFunction() == rhs.getJoinFunction() and getOutputSchema() == rhs.outputSchema
        and getRightSchema() == rhs.getRightSchema() and getLeftSchema() == rhs.getLeftSchema() and getTraitSet() == rhs.getTraitSet();
}

std::string JoinLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "Join(opId: {}, windowType: {}, joinFunction: {}, windowStartField: {}, windowEndField: {}, traitSet: {})",
            id,
            getWindowType()->toString(),
            getJoinFunction().explain(verbosity),
            windowMetaData.windowStartFieldName,
            windowMetaData.windowEndFieldName,
            traitSet.explain(verbosity));
    }
    return fmt::format("Join({})", getJoinFunction().explain(verbosity));
}

JoinLogicalOperator JoinLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    const auto& leftInputSchema = inputSchemas[0];
    const auto& rightInputSchema = inputSchemas[1];

    auto copy = *this;
    copy.outputSchema = Schema{};
    copy.leftInputSchema = leftInputSchema;
    copy.rightInputSchema = rightInputSchema;

    const auto newQualifierForSystemField = [](const Schema& leftSchema, const Schema& rightSchema)
    {
        const auto sourceNameLeft = leftSchema.getSourceNameQualifier();
        const auto sourceNameRight = rightSchema.getSourceNameQualifier();
        if (not(sourceNameLeft and sourceNameRight))
        {
            throw TypeInferenceException("Schemas of Join operator must have source names.");
        }
        return sourceNameLeft.value() + sourceNameRight.value() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    }(leftInputSchema, rightInputSchema);

    copy.windowMetaData.windowStartFieldName = newQualifierForSystemField + "START";
    copy.windowMetaData.windowEndFieldName = newQualifierForSystemField + "END";
    copy.outputSchema.addField(copy.windowMetaData.windowStartFieldName, DataType::Type::UINT64);
    copy.outputSchema.addField(copy.windowMetaData.windowEndFieldName, DataType::Type::UINT64);

    for (const auto& field : leftInputSchema.getFields())
    {
        copy.outputSchema.addField(field.name, field.dataType);
    }
    for (const auto& field : rightInputSchema.getFields())
    {
        copy.outputSchema.addField(field.name, field.dataType);
    }

    auto inputSchema = leftInputSchema;
    inputSchema.appendFieldsFromOtherSchema(rightInputSchema);
    copy.joinFunction = joinFunction.withInferredDataType(inputSchema);
    copy.windowType->inferStamp(inputSchema);
    return copy;
}

JoinLogicalOperator JoinLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = traitSet;
    return copy;
}

TraitSet JoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

JoinLogicalOperator JoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
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

Reflected Reflector<JoinLogicalOperator>::operator()(const JoinLogicalOperator& op) const
{
    return reflect(detail::ReflectedJoinLogicalOperator{
        .joinFunction = op.getJoinFunction(), .windowType = reflectWindowType(op.getWindowType()), .joinType = op.joinType});
}

JoinLogicalOperator Unreflector<JoinLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [joinFunction, reflectedWindowType, joinType] = unreflect<detail::ReflectedJoinLogicalOperator>(reflected);

    if (!joinFunction.has_value())
    {
        throw CannotDeserialize("Missing Join Function");
    }

    const auto windowType = unreflectWindowType(reflectedWindowType);

    return JoinLogicalOperator(joinFunction.value(), windowType, joinType);
}

LogicalOperatorRegistryReturnType LogicalOperatorGeneratedRegistrar::RegisterJoinLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<JoinLogicalOperator>(arguments.reflected);
    }

    if (arguments.inputSchemas.size() != 2)
    {
        throw CannotDeserialize("Expected two input schemas, but got {}", arguments.inputSchemas.size());
    }

    auto functionVariant = arguments.config.at(JoinLogicalOperator::ConfigParameters::JOIN_FUNCTION);
    auto joinTypeVariant = arguments.config.at(JoinLogicalOperator::ConfigParameters::JOIN_TYPE);
    auto windowInfoVariant = arguments.config.at(JoinLogicalOperator::ConfigParameters::WINDOW_INFOS);
    auto windowStartVariant = arguments.config.at(JoinLogicalOperator::ConfigParameters::WINDOW_START_FIELD_NAME);
    auto windowEndVariant = arguments.config.at(JoinLogicalOperator::ConfigParameters::WINDOW_END_FIELD_NAME);

    if (std::holds_alternative<FunctionList>(functionVariant) and std::holds_alternative<EnumWrapper>(joinTypeVariant)
        and std::holds_alternative<std::string>(windowStartVariant) and std::holds_alternative<std::string>(windowEndVariant))
    {
        auto functions = std::get<FunctionList>(functionVariant).functions();
        auto joinType = std::get<EnumWrapper>(joinTypeVariant);
        auto windowStartFieldName = std::get<std::string>(windowStartVariant);
        auto windowEndFieldName = std::get<std::string>(windowEndVariant);

        if (functions.size() != 1)
        {
            throw CannotDeserialize("Expected exactly one function");
        }
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
            else
            {
                throw CannotDeserialize("Neither tumling nor sliding window");
            }
        }
        if (!windowType)
        {
            throw UnknownLogicalOperator();
        }

        auto logicalOperator = JoinLogicalOperator(function, windowType, joinType.asEnum<JoinLogicalOperator::JoinType>().value());
        return logicalOperator.withInferredSchema(arguments.inputSchemas);
    }
    throw UnknownLogicalOperator();
}

}
