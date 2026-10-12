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

#include <Functions/ToInt64LogicalFunction.hpp>

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

ToInt64LogicalFunction::ToInt64LogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE), child(child)
{
}

DataType ToInt64LogicalFunction::getDataType() const
{
    return dataType;
};

ToInt64LogicalFunction ToInt64LogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction ToInt64LogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "ToInt64LogicalFunction expects exactly one child function but has {}", newChildren.size());
    INVARIANT(newChildren[0].getDataType().isNumeric(), "TOINT64 requires a numeric argument, got {}", newChildren[0].getDataType());
    auto newDataType = DataType(DataType::Type::INT64, DataType::NULLABLE::NOT_NULLABLE);
    newDataType.nullable = newChildren[0].getDataType().nullable;
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> ToInt64LogicalFunction::getChildren() const
{
    return {child};
};

ToInt64LogicalFunction ToInt64LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "ToInt64LogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view ToInt64LogicalFunction::getType() const
{
    return NAME;
}

bool ToInt64LogicalFunction::operator==(const ToInt64LogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string ToInt64LogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ToInt64LogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("TOINT64({})", child.explain(verbosity));
}

Reflected Reflector<ToInt64LogicalFunction>::operator()(const ToInt64LogicalFunction& function) const
{
    return reflect(detail::ReflectedToInt64LogicalFunction{.child = function.child});
}

ToInt64LogicalFunction Unreflector<ToInt64LogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedToInt64LogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("ToInt64LogicalFunction is missing its child");
    }
    return ToInt64LogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterToInt64LogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ToInt64LogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("ToInt64LogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return ToInt64LogicalFunction(arguments.children[0]);
}

}
