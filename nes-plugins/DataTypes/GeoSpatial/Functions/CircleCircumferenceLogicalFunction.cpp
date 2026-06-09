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

#include <CircleCircumferenceLogicalFunction.hpp>

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

CircleCircumferenceLogicalFunction::CircleCircumferenceLogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), child(child)
{
}

DataType CircleCircumferenceLogicalFunction::getDataType() const
{
    return dataType;
}

CircleCircumferenceLogicalFunction CircleCircumferenceLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction CircleCircumferenceLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "CircleCircumferenceLogicalFunction expects exactly one child function but has {}", newChildren.size());
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
            "CircleCircumference expects a Circle data type as child, instead got: ", childType);
    }
    const auto nullable = childType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::FLOAT64, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> CircleCircumferenceLogicalFunction::getChildren() const
{
    return {child};
}

CircleCircumferenceLogicalFunction CircleCircumferenceLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CircleCircumferenceLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view CircleCircumferenceLogicalFunction::getType() const
{
    return NAME;
}

bool CircleCircumferenceLogicalFunction::operator==(const CircleCircumferenceLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string CircleCircumferenceLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("CircleCircumferenceLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("circle_circumference({})", child.explain(verbosity));
}

Reflected Reflector<CircleCircumferenceLogicalFunction>::operator()(const CircleCircumferenceLogicalFunction& function) const
{
    return reflect(detail::ReflectedCircleCircumferenceLogicalFunction{.child = function.child});
}

CircleCircumferenceLogicalFunction Unreflector<CircleCircumferenceLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedCircleCircumferenceLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("CircleCircumferenceLogicalFunction is missing its child");
    }
    return CircleCircumferenceLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterCIRCLE_CIRCUMFERENCELogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<CircleCircumferenceLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("CircleCircumferenceLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return CircleCircumferenceLogicalFunction(arguments.children[0]);
}
}
