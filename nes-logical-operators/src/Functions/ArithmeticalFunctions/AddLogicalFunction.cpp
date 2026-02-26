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

#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

AddLogicalFunction::AddLogicalFunction(const LogicalFunction& left, const LogicalFunction& right)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), left(left), right(right)
{
}

DataType AddLogicalFunction::getDataType() const
{
    return dataType;
};

AddLogicalFunction AddLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction AddLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 2, "AddLogicalFunction expects exactly two child function but has {}", newChildren.size());
    auto newDataType = newChildren[0].getDataType().join(newChildren[1].getDataType());
    if (not newDataType.has_value())
    {
        throw DifferentFieldTypeExpected("Could not join {} and {}", newChildren[0].getDataType(), newChildren[1].getDataType());
    }
    newDataType.value().nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType.value()).withChildren(newChildren);
}

std::vector<LogicalFunction> AddLogicalFunction::getChildren() const
{
    return {left, right};
};

AddLogicalFunction AddLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "AddLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType
        = children[0].getDataType().join(children[1].getDataType()).value_or(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED));
    return copy;
};

std::string_view AddLogicalFunction::getType() const
{
    return NAME;
}

bool AddLogicalFunction::operator==(const AddLogicalFunction& rhs) const
{
    const bool simpleMatch = left == rhs.left and right == rhs.right;
    const bool commutativeMatch = left == rhs.right and right == rhs.left;
    return simpleMatch or commutativeMatch;
}

std::string AddLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("AddLogicalFunction({} + {} : {})", left.explain(verbosity), right.explain(verbosity), dataType);
    }
    return fmt::format("{} + {}", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<AddLogicalFunction>::operator()(const AddLogicalFunction& function) const
{
    return reflect(detail::ReflectedAddLogicalFunction{.left = function.left, .right = function.right});
}

AddLogicalFunction Unreflector<AddLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedAddLogicalFunction>(reflected);
    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("AddLogicalFunction is missing child");
    }
    return AddLogicalFunction{left.value(), right.value()};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAddLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<AddLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Function requires exactly two children, but got {}", arguments.children.size());
    }
    return AddLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
