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

#include <CircleAreaLogicalFunction.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

CircleAreaLogicalFunction::CircleAreaLogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), child(child)
{
}

DataType CircleAreaLogicalFunction::getDataType() const
{
    return dataType;
}

CircleAreaLogicalFunction CircleAreaLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction CircleAreaLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "CircleAreaLogicalFunction expects exactly one child function but has {}", newChildren.size());
    const auto childType = newChildren[0].getDataType();
    /// Check if the child is of the circle datatype structure
    if (childType.type != DataType::Type::STRUCT || childType.fields.size() != 2
        || childType.fields[0].first != "center"
        || childType.fields[0].second.type != DataType::Type::STRUCT
        || childType.fields[0].second.fields.size() != 2
        || childType.fields[0].second.fields[0].first != "x"
        || childType.fields[0].second.fields[0].second.type != DataType::Type::FLOAT64
        || childType.fields[0].second.fields[1].first != "y"
        || childType.fields[0].second.fields[1].second.type != DataType::Type::FLOAT64
        || childType.fields[1].first != "radius"
        || childType.fields[1].second.type != DataType::Type::FLOAT64)
    {
        throw DifferentFieldTypeExpected(
            "CircleArea expects a Circle data type as child, instead got: ", childType);
    }
    const auto nullable = childType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::FLOAT64, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> CircleAreaLogicalFunction::getChildren() const
{
    return {child};
}

CircleAreaLogicalFunction CircleAreaLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CircleAreaLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view CircleAreaLogicalFunction::getType() const
{
    return NAME;
}

bool CircleAreaLogicalFunction::operator==(const CircleAreaLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string CircleAreaLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("CircleAreaLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("circle_area({})", child.explain(verbosity));
}

Reflected Reflector<CircleAreaLogicalFunction>::operator()(const CircleAreaLogicalFunction& function) const
{
    return reflect(detail::ReflectedCircleAreaLogicalFunction{.child = function.child});
}

CircleAreaLogicalFunction Unreflector<CircleAreaLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedCircleAreaLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("CircleAreaLogicalFunction is missing its child");
    }
    return CircleAreaLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterCIRCLE_AREALogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<CircleAreaLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("CircleAreaLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return CircleAreaLogicalFunction(arguments.children[0]);
}
}
