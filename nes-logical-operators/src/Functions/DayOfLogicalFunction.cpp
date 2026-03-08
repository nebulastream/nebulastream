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

#include <Functions/DayOfLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
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

DayOfLogicalFunction::DayOfLogicalFunction(LogicalFunction child)
    : outputType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), child(std::move(child))
{
}

bool DayOfLogicalFunction::operator==(const DayOfLogicalFunction& rhs) const
{
    return this->child == rhs.child;
}

DataType DayOfLogicalFunction::getDataType() const
{
    return outputType;
}

DayOfLogicalFunction DayOfLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.outputType = dataType;
    return copy;
}

LogicalFunction DayOfLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "DayOfLogicalFunction expects exactly one child function but has {}", newChildren.size());
    auto newDataType = newChildren[0].getDataType();
    if (newDataType.type != DataType::Type::UNDEFINED && newDataType.type != DataType::Type::UINT64)
    {
        throw DifferentFieldTypeExpected("DAY_OF expects a UINT64 input, but got {}", newDataType);
    }

    newDataType.type = DataType::Type::UINT64;
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> DayOfLogicalFunction::getChildren() const
{
    return {child};
}

DayOfLogicalFunction DayOfLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "DayOfLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view DayOfLogicalFunction::getType()
{
    return NAME;
}

std::string DayOfLogicalFunction::explain(ExplainVerbosity) const
{
    return fmt::format("Extract day from unix timestamp (ms), outputType={}", outputType);
}

Reflected Reflector<DayOfLogicalFunction>::operator()(const DayOfLogicalFunction& function) const
{
    return reflect(detail::ReflectedDayOfLogicalFunction{.child = function.child});
}

DayOfLogicalFunction Unreflector<DayOfLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [function] = context.unreflect<detail::ReflectedDayOfLogicalFunction>(reflected);

    return DayOfLogicalFunction{std::move(function)};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterDay_OfLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("DayOfLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return DayOfLogicalFunction{arguments.children[0]};
}

}
