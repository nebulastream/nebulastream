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
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
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

JoinLogicalOperator::JoinLogicalOperator(LogicalFunction joinFunction, std::shared_ptr<Windowing::WindowType> windowType, JoinType joinType)
{
    data.joinFunction = std::move(joinFunction);
    data.windowType = std::move(windowType);
    data.joinType = joinType;
}

std::string_view JoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string JoinLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "Join(opId: {}, windowType: {}, joinFunction: {}, windowStartField: {}, windowEndField: {})",
            id,
            getWindowType()->toString(),
            getJoinFunction().explain(verbosity),
            data.windowMetaData.windowStartFieldName,
            data.windowMetaData.windowEndFieldName);
    }
    return fmt::format("Join({})", getJoinFunction().explain(verbosity));
}

LogicalOperator JoinLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    const auto& leftInputSchema = inputSchemas[0];
    const auto& rightInputSchema = inputSchemas[1];

    auto copy = *this;
    copy.outputSchema = Schema{copy.outputSchema.memoryLayoutType};
    copy.inputSchemas = inputSchemas;

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

    copy.data.windowMetaData.windowStartFieldName = newQualifierForSystemField + "start";
    copy.data.windowMetaData.windowEndFieldName = newQualifierForSystemField + "end";
    copy.outputSchema.addField(copy.data.windowMetaData.windowStartFieldName, DataType::Type::UINT64);
    copy.outputSchema.addField(copy.data.windowMetaData.windowEndFieldName, DataType::Type::UINT64);

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
    copy.data.joinFunction = data.joinFunction.withInferredDataType(inputSchema);
    copy.data.windowType->inferStamp(inputSchema);
    return copy;
}

Schema JoinLogicalOperator::getLeftSchema() const
{
    return inputSchemas[0];
}

Schema JoinLogicalOperator::getRightSchema() const
{
    return inputSchemas[1];
}

std::shared_ptr<Windowing::WindowType> JoinLogicalOperator::getWindowType() const
{
    return data.windowType;
}

std::string JoinLogicalOperator::getWindowStartFieldName() const
{
    return data.windowMetaData.windowStartFieldName;
}

std::string JoinLogicalOperator::getWindowEndFieldName() const
{
    return data.windowMetaData.windowEndFieldName;
}

const WindowMetaData& JoinLogicalOperator::getWindowMetaData() const
{
    return data.windowMetaData;
}

LogicalFunction JoinLogicalOperator::getJoinFunction() const
{
    return data.joinFunction;
}



}
