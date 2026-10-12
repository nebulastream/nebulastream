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

#include <Functions/ToFloat64LogicalFunction.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
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

ToFloat64LogicalFunction::ToFloat64LogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE), child(child)
{
}

DataType ToFloat64LogicalFunction::getDataType() const
{
    return dataType;
};

ToFloat64LogicalFunction ToFloat64LogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction ToFloat64LogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "ToFloat64LogicalFunction expects exactly one child function but has {}", newChildren.size());
    INVARIANT(newChildren[0].getDataType().isNumeric(), "TOFLOAT64 requires a numeric argument, got {}", newChildren[0].getDataType());
    auto newDataType = DataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE);
    newDataType.nullable = newChildren[0].getDataType().nullable;
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> ToFloat64LogicalFunction::getChildren() const
{
    return {child};
};

ToFloat64LogicalFunction ToFloat64LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "ToFloat64LogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view ToFloat64LogicalFunction::getType() const
{
    return NAME;
}

bool ToFloat64LogicalFunction::operator==(const ToFloat64LogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string ToFloat64LogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ToFloat64LogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("TOFLOAT64({})", child.explain(verbosity));
}

Reflected Reflector<ToFloat64LogicalFunction>::operator()(const ToFloat64LogicalFunction& function) const
{
    return reflect(detail::ReflectedToFloat64LogicalFunction{.child = function.child});
}

ToFloat64LogicalFunction Unreflector<ToFloat64LogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedToFloat64LogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("ToFloat64LogicalFunction is missing its child");
    }
    return ToFloat64LogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterToFloat64LogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ToFloat64LogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("ToFloat64LogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return ToFloat64LogicalFunction(arguments.children[0]);
}

}
