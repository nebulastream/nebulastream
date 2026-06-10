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

#include <PointCircleContainsLogicalFuction.hpp>

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

PointCircleContainsLogicalFunction::PointCircleContainsLogicalFunction(const LogicalFunction& leftChild, const LogicalFunction& rightChild)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), leftChild(leftChild), rightChild(rightChild)
{
}

DataType PointCircleContainsLogicalFunction::getDataType() const
{
    return dataType;
}

PointCircleContainsLogicalFunction PointCircleContainsLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction PointCircleContainsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(
        newChildren.size() == 2, "PointCircleContainsLogicalFunction expects exactly two child functions but has {}", newChildren.size());
    const auto leftType = newChildren[0].getDataType();
    const auto rightType = newChildren[1].getDataType();
    /// Check if the left child is of the point datatype structure and the right is of the circle datatype structure
    if (rightType.type != DataType::Type::STRUCT || rightType.fields.size() != 2 || rightType.fields[0].first != "center"
        || rightType.fields[0].second.type != DataType::Type::STRUCT || rightType.fields[0].second.fields.size() != 2
        || rightType.fields[0].second.fields[0].first != "x" || rightType.fields[0].second.fields[0].second.type != DataType::Type::FLOAT64
        || rightType.fields[0].second.fields[1].first != "y" || rightType.fields[0].second.fields[1].second.type != DataType::Type::FLOAT64
        || rightType.fields[1].first != "radius" || rightType.fields[1].second.type != DataType::Type::FLOAT64
        || leftType.type != DataType::Type::STRUCT || leftType.fields.size() != 2 || leftType.fields[0].first != "x"
        || leftType.fields[0].second.type != DataType::Type::FLOAT64 || leftType.fields[1].first != "y"
        || leftType.fields[1].second.type != DataType::Type::FLOAT64)
    {
        throw DifferentFieldTypeExpected(
            "PointCircleContains expects a Point data type as left child and a Circle type as right child, instead got: ",
            leftType,
            rightType);
    }
    const auto nullable = leftType.nullable || rightType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::BOOLEAN, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> PointCircleContainsLogicalFunction::getChildren() const
{
    return {leftChild, rightChild};
}

PointCircleContainsLogicalFunction PointCircleContainsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "PointCircleContainsLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.leftChild = children[0];
    copy.rightChild = children[1];
    return copy;
}

std::string_view PointCircleContainsLogicalFunction::getType() const
{
    return NAME;
}

bool PointCircleContainsLogicalFunction::operator==(const PointCircleContainsLogicalFunction& rhs) const
{
    return leftChild == rhs.leftChild && rightChild == rhs.rightChild;
}

std::string PointCircleContainsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "PointCircleContainsLogicalFunction({}, {} : {})", leftChild.explain(verbosity), rightChild.explain(verbosity), dataType);
    }
    return fmt::format("point_circle_contains({}, {})", leftChild.explain(verbosity), rightChild.explain(verbosity));
}

Reflected Reflector<PointCircleContainsLogicalFunction>::operator()(const PointCircleContainsLogicalFunction& function) const
{
    return reflect(detail::ReflectedPointCircleContainsLogicalFunction{.left = function.leftChild, .right = function.rightChild});
}

PointCircleContainsLogicalFunction Unreflector<PointCircleContainsLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedPointCircleContainsLogicalFunction>(reflected);
    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("PointCircleContainsLogicalFunction is missing its child");
    }
    return PointCircleContainsLogicalFunction(left.value(), right.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPOINT_CIRCLE_CONTAINSLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointCircleContainsLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("PointCircleContainsLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return PointCircleContainsLogicalFunction(arguments.children[0], arguments.children[1]);
}
}
