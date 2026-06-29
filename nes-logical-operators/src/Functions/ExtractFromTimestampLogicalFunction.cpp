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

#include <Functions/ExtractFromTimestampLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

namespace
{
constexpr std::string_view DAY_OF_NAME = "Day_Of";
constexpr std::string_view MONTH_OF_NAME = "Month_Of";
constexpr std::string_view YEAR_OF_NAME = "Year_Of";
}

ExtractFromTimestampLogicalFunction::ExtractFromTimestampLogicalFunction(TimestampUnit unit, LogicalFunction child)
    : unit(unit), outputType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), child(std::move(child))
{
}

bool ExtractFromTimestampLogicalFunction::operator==(const ExtractFromTimestampLogicalFunction& rhs) const
{
    return this->unit == rhs.unit && this->child == rhs.child;
}

DataType ExtractFromTimestampLogicalFunction::getDataType() const
{
    return outputType;
}

ExtractFromTimestampLogicalFunction ExtractFromTimestampLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.outputType = dataType;
    return copy;
}

LogicalFunction ExtractFromTimestampLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto inferredChild = child.withInferredDataType(schema);
    auto childDataType = inferredChild.getDataType();
    if (childDataType.type != DataType::Type::UNDEFINED && childDataType.type != DataType::Type::UINT64)
    {
        throw DifferentFieldTypeExpected("{} expects a UINT64 input, but got {}", getType(), childDataType);
    }

    auto inferredOutput = childDataType;
    inferredOutput.type = DataType::Type::UINT64;
    inferredOutput.nullable = childDataType.nullable;

    auto copy = *this;
    copy.child = std::move(inferredChild);
    copy.outputType = inferredOutput;
    return copy;
}

std::vector<LogicalFunction> ExtractFromTimestampLogicalFunction::getChildren() const
{
    return {child};
}

ExtractFromTimestampLogicalFunction ExtractFromTimestampLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "ExtractFromTimestampLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

TimestampUnit ExtractFromTimestampLogicalFunction::getUnit() const
{
    return unit;
}

std::string_view ExtractFromTimestampLogicalFunction::getType() const
{
    switch (unit)
    {
        case TimestampUnit::Day:
            return DAY_OF_NAME;
        case TimestampUnit::Month:
            return MONTH_OF_NAME;
        case TimestampUnit::Year:
            return YEAR_OF_NAME;
    }
    INVARIANT(false, "Unknown TimestampUnit value: {}", static_cast<int>(unit));
    std::unreachable();
}

std::string ExtractFromTimestampLogicalFunction::explain(ExplainVerbosity) const
{
    return fmt::format("{}: extract from unix timestamp (ms), outputType={}", getType(), outputType);
}

Reflected Reflector<ExtractFromTimestampLogicalFunction>::operator()(
    const ExtractFromTimestampLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedExtractFromTimestampLogicalFunction{.unit = function.unit, .child = function.child});
}

ExtractFromTimestampLogicalFunction
Unreflector<ExtractFromTimestampLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [unit, child] = context.unreflect<detail::ReflectedExtractFromTimestampLogicalFunction>(reflected);
    return ExtractFromTimestampLogicalFunction{unit, std::move(child)};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterDay_OfLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize(
            "ExtractFromTimestampLogicalFunction (Day_Of) requires exactly one child, but got {}", arguments.children.size());
    }
    return ExtractFromTimestampLogicalFunction{TimestampUnit::Day, arguments.children[0]};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterMonth_OfLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize(
            "ExtractFromTimestampLogicalFunction (Month_Of) requires exactly one child, but got {}", arguments.children.size());
    }
    return ExtractFromTimestampLogicalFunction{TimestampUnit::Month, arguments.children[0]};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterYear_OfLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize(
            "ExtractFromTimestampLogicalFunction (Year_Of) requires exactly one child, but got {}", arguments.children.size());
    }
    return ExtractFromTimestampLogicalFunction{TimestampUnit::Year, arguments.children[0]};
}

}
