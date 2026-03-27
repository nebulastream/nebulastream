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

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

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
    if (isOuterJoin(this->joinType)
        && std::ranges::none_of(
            BFSRange(this->joinFunction), [](const LogicalFunction& fn) { return fn.tryGetAs<FieldAccessLogicalFunction>().has_value(); }))
    {
        throw UnsupportedQuery("Outer joins require an explicit equi-join predicate (e.g. ON left$key = right$key)");
    }
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
    const auto joinTypeName = std::string(magic_enum::enum_name(joinType));
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "Join(opId: {}, type: {}, windowType: {}, joinFunction: {}, windowStartField: {}, windowEndField: {}, traitSet: {})",
            id,
            joinTypeName,
            getWindowType()->toString(),
            getJoinFunction().explain(verbosity),
            windowMetaData.windowStartFieldName,
            windowMetaData.windowEndFieldName,
            traitSet.explain(verbosity));
    }
    return fmt::format("Join({}, {})", joinTypeName, getJoinFunction().explain(verbosity));
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

    const bool leftNullable = (joinType == JoinType::OUTER_RIGHT_JOIN || joinType == JoinType::OUTER_FULL_JOIN);
    const bool rightNullable = (joinType == JoinType::OUTER_LEFT_JOIN || joinType == JoinType::OUTER_FULL_JOIN);

    for (const auto& field : leftInputSchema.getFields())
    {
        if (leftNullable)
        {
            copy.outputSchema.addField(field.name, field.dataType.type, DataType::NULLABLE::IS_NULLABLE);
        }
        else
        {
            copy.outputSchema.addField(field.name, field.dataType);
        }
    }
    for (const auto& field : rightInputSchema.getFields())
    {
        if (rightNullable)
        {
            copy.outputSchema.addField(field.name, field.dataType.type, DataType::NULLABLE::IS_NULLABLE);
        }
        else
        {
            copy.outputSchema.addField(field.name, field.dataType);
        }
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

JoinLogicalOperator::JoinType JoinLogicalOperator::getJoinType() const
{
    return joinType;
}

Reflected Reflector<JoinLogicalOperator>::operator()(const JoinLogicalOperator& op) const
{
    return reflect(detail::ReflectedJoinLogicalOperator{
        .joinFunction = op.getJoinFunction(), .windowType = reflectWindowType(*op.getWindowType()), .joinType = op.joinType});
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

    PRECONDITION(false, "Operator is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
