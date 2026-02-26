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

#include <Functions/ArithmeticalFunctions/FloorLogicalFunction.hpp>

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

FloorLogicalFunction::FloorLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child) { };

DataType FloorLogicalFunction::getDataType() const
{
    return dataType;
};

FloorLogicalFunction FloorLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction FloorLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "FloorLogicalFunction expects exactly one child function but has {}", newChildren.size());
    auto newDataType = newChildren[0].getDataType();
    newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& child) { return child.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
};

std::vector<LogicalFunction> FloorLogicalFunction::getChildren() const
{
    return {child};
};

FloorLogicalFunction FloorLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "FloorLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view FloorLogicalFunction::getType() const
{
    return NAME;
}

bool FloorLogicalFunction::operator==(const FloorLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string FloorLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FloorLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("FLOOR({})", child.explain(verbosity));
}

Reflected Reflector<FloorLogicalFunction>::operator()(const FloorLogicalFunction& function) const
{
    return reflect(detail::ReflectedFloorLogicalFunction{.child = function.child});
}

FloorLogicalFunction Unreflector<FloorLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedFloorLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("Missing child function");
    }
    return FloorLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFloorLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<FloorLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Function requires exactly one child, but got {}", arguments.children.size());
    }
    return FloorLogicalFunction(arguments.children[0]);
}

}
