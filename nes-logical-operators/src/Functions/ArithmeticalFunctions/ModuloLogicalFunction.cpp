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

#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>

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

ModuloLogicalFunction::ModuloLogicalFunction(const LogicalFunction& left, const LogicalFunction& right)
    : logicalType(LogicalType{"UNDEFINED", {}, Nullable::IS_NULLABLE}), left(left), right(right)
{
}

bool ModuloLogicalFunction::operator==(const ModuloLogicalFunction& rhs) const
{
    return left == rhs.left and right == rhs.right;
}

LogicalType ModuloLogicalFunction::getLogicalType() const
{
    return logicalType;
};

ModuloLogicalFunction ModuloLogicalFunction::withLogicalType(const LogicalType& logicalType) const
{
    auto copy = *this;
    copy.logicalType = logicalType;
    return copy;
};

LogicalFunction ModuloLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredLogicalType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 2, "ModuloLogicalFunction expects exactly two child function but has {}", newChildren.size());
    auto newDataType = newChildren[0].getLogicalType().join(newChildren[1].getLogicalType());
    if (not newDataType.has_value())
    {
        throw DifferentFieldTypeExpected("Could not join {} and {}", newChildren[0].getLogicalType(), newChildren[1].getLogicalType());
    }
    return withLogicalType(newDataType.value()).withChildren(newChildren);
};

std::vector<LogicalFunction> ModuloLogicalFunction::getChildren() const
{
    return {left, right};
};

ModuloLogicalFunction ModuloLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "ModuloLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.logicalType
        = children[0].getLogicalType().join(children[1].getLogicalType()).value_or(LogicalType{"UNDEFINED", {}, Nullable::IS_NULLABLE});
    return copy;
};

std::string_view ModuloLogicalFunction::getType() const
{
    return NAME;
}

std::string ModuloLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ModuloLogicalFunction({} % {} : {})", left.explain(verbosity), right.explain(verbosity), logicalType);
    }
    return fmt::format("{} % {}", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<ModuloLogicalFunction>::operator()(const ModuloLogicalFunction& function) const
{
    return reflect(detail::ReflectedModuloLogicalFunction{.left = function.left, .right = function.right});
}

ModuloLogicalFunction Unreflector<ModuloLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [left, right] = unreflect<detail::ReflectedModuloLogicalFunction>(reflected);
    if (!left.has_value() || !right.has_value())
    {
        throw CannotDeserialize("Missing child function");
    }
    return ModuloLogicalFunction{left.value(), right.value()};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterModLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ModuloLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("ModuloLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    }
    return ModuloLogicalFunction(arguments.children[0], arguments.children[1]);
}


}
