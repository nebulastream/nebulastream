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

#include <NearestApproachDistanceLogicalFunction.hpp>

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

NearestApproachDistanceLogicalFunction::NearestApproachDistanceLogicalFunction(
    const LogicalFunction& leftChild, const LogicalFunction& rightChild)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), leftChild(leftChild), rightChild(rightChild)
{
}

DataType NearestApproachDistanceLogicalFunction::getDataType() const
{
    return dataType;
}

NearestApproachDistanceLogicalFunction NearestApproachDistanceLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction NearestApproachDistanceLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(
        newChildren.size() == 2,
        "NearestApproachDistanceLogicalFunction expects exactly two child functions but has {}",
        newChildren.size());
    const auto leftType = newChildren[0].getDataType();
    const auto rightType = newChildren[1].getDataType();
    const auto nullable = leftType.nullable || rightType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::FLOAT64, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> NearestApproachDistanceLogicalFunction::getChildren() const
{
    return {leftChild, rightChild};
}

NearestApproachDistanceLogicalFunction NearestApproachDistanceLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "NearestApproachDistanceLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.leftChild = children[0];
    copy.rightChild = children[1];
    return copy;
}

std::string_view NearestApproachDistanceLogicalFunction::getType() const
{
    return NAME;
}

bool NearestApproachDistanceLogicalFunction::operator==(const NearestApproachDistanceLogicalFunction& rhs) const
{
    return leftChild == rhs.leftChild && rightChild == rhs.rightChild;
}

std::string NearestApproachDistanceLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "NearestApproachDistanceLogicalFunction({}, {} : {})", leftChild.explain(verbosity), rightChild.explain(verbosity), dataType);
    }
    return fmt::format("nearest_approach_distance({}, {})", leftChild.explain(verbosity), rightChild.explain(verbosity));
}

Reflected Reflector<NearestApproachDistanceLogicalFunction>::operator()(const NearestApproachDistanceLogicalFunction& function) const
{
    return reflect(detail::ReflectedNearestApproachDistanceLogicalFunction{.left = function.leftChild, .right = function.rightChild});
}

NearestApproachDistanceLogicalFunction Unreflector<NearestApproachDistanceLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedNearestApproachDistanceLogicalFunction>(reflected);
    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("NearestApproachDistanceLogicalFunction is missing its child");
    }
    return NearestApproachDistanceLogicalFunction(left.value(), right.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterNEAREST_APPROACH_DISTANCELogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<NearestApproachDistanceLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("NearestApproachDistanceLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return NearestApproachDistanceLogicalFunction(arguments.children[0], arguments.children[1]);
}
}
