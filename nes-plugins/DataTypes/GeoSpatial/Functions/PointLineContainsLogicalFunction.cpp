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

#include <PointLineContainsLogicalFunction.hpp>

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

PointLineContainsLogicalFunction::PointLineContainsLogicalFunction(const LogicalFunction& leftChild, const LogicalFunction& rightChild)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), leftChild(leftChild), rightChild(rightChild)
{
}

DataType PointLineContainsLogicalFunction::getDataType() const
{
    return dataType;
}

PointLineContainsLogicalFunction PointLineContainsLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction PointLineContainsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(
        newChildren.size() == 2, "PointLineContainsLogicalFunction expects exactly two child functions but has {}", newChildren.size());
    const auto leftType = newChildren[0].getDataType();
    const auto rightType = newChildren[1].getDataType();
    /// Check if the left child is of the point datatype structure and the right is of the circle datatype structure
    if (rightType.type != DataType::Type::STRUCT || rightType.fields.size() != 3 || rightType.fields[0].first != "A"
        || rightType.fields[0].second.type != DataType::Type::FLOAT64 || rightType.fields[1].first != "B"
        || rightType.fields[1].second.type != DataType::Type::FLOAT64 || rightType.fields[2].first != "C"
        || rightType.fields[2].second.type != DataType::Type::FLOAT64 || leftType.type != DataType::Type::STRUCT
        || leftType.fields.size() != 2 || leftType.fields[0].first != "x" || leftType.fields[0].second.type != DataType::Type::FLOAT64
        || leftType.fields[1].first != "y" || leftType.fields[1].second.type != DataType::Type::FLOAT64)
    {
        throw DifferentFieldTypeExpected(
            "PointLineContains expects a Point data type as left child and a Line type as right child, instead got: ",
            leftType,
            rightType);
    }
    const auto nullable = leftType.nullable || rightType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::BOOLEAN, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> PointLineContainsLogicalFunction::getChildren() const
{
    return {leftChild, rightChild};
}

PointLineContainsLogicalFunction PointLineContainsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "PointLineContainsLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.leftChild = children[0];
    copy.rightChild = children[1];
    return copy;
}

std::string_view PointLineContainsLogicalFunction::getType() const
{
    return NAME;
}

bool PointLineContainsLogicalFunction::operator==(const PointLineContainsLogicalFunction& rhs) const
{
    return leftChild == rhs.leftChild && rightChild == rhs.rightChild;
}

std::string PointLineContainsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "PointLineContainsLogicalFunction({}, {} : {})", leftChild.explain(verbosity), rightChild.explain(verbosity), dataType);
    }
    return fmt::format("point_line_contains({}, {})", leftChild.explain(verbosity), rightChild.explain(verbosity));
}

Reflected Reflector<PointLineContainsLogicalFunction>::operator()(const PointLineContainsLogicalFunction& function) const
{
    return reflect(detail::ReflectedPointLineContainsLogicalFunction{.left = function.leftChild, .right = function.rightChild});
}

PointLineContainsLogicalFunction Unreflector<PointLineContainsLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedPointLineContainsLogicalFunction>(reflected);
    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("PointLineContainsLogicalFunction is missing its child");
    }
    return PointLineContainsLogicalFunction(left.value(), right.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPOINT_LINE_CONTAINSLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointLineContainsLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("PointLineContainsLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return PointLineContainsLogicalFunction(arguments.children[0], arguments.children[1]);
}
}
