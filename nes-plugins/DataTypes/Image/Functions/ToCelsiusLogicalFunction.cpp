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

#include <ToCelsiusLogicalFunction.hpp>

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

ToCelsiusLogicalFunction::ToCelsiusLogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), child(child)
{
}

DataType ToCelsiusLogicalFunction::getDataType() const
{
    return dataType;
}

ToCelsiusLogicalFunction ToCelsiusLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ToCelsiusLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "ToCelsiusLogicalFunction expects exactly one child function but has {}", newChildren.size());
    /// Layout gate: any STRUCT with a single FIXEDSIZED<UINT16> field named
    /// "pixels" works (e.g. both `ThermalFrame` and `ThermalImage`). Output is
    /// FIXEDSIZED<FLOAT32, count> derived from the frame's pixels field.
    const auto childType = newChildren[0].getDataType();
    if (childType.type != DataType::Type::STRUCT || childType.fields.size() != 1 || childType.fields[0].first != "pixels"
        || childType.fields[0].second.type != DataType::Type::FIXEDSIZED
        || childType.fields[0].second.elementType != DataType::Type::UINT16)
    {
        throw DifferentFieldTypeExpected("to_celsius expects a STRUCT with a single FIXEDSIZED<UINT16> 'pixels' field, got {}", childType);
    }
    const auto pixelCount = childType.fields[0].second.count;
    const auto nullable = childType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::FIXEDSIZED, nullable, DataType::Type::FLOAT32, pixelCount};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> ToCelsiusLogicalFunction::getChildren() const
{
    return {child};
}

ToCelsiusLogicalFunction ToCelsiusLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "ToCelsiusLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view ToCelsiusLogicalFunction::getType() const
{
    return NAME;
}

bool ToCelsiusLogicalFunction::operator==(const ToCelsiusLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string ToCelsiusLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ToCelsiusLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("to_celsius({})", child.explain(verbosity));
}

Reflected Reflector<ToCelsiusLogicalFunction>::operator()(const ToCelsiusLogicalFunction& function) const
{
    return reflect(detail::ReflectedToCelsiusLogicalFunction{.child = function.child});
}

ToCelsiusLogicalFunction Unreflector<ToCelsiusLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedToCelsiusLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("ToCelsiusLogicalFunction is missing its child");
    }
    return ToCelsiusLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTO_CELSIUSLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ToCelsiusLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("ToCelsiusLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return ToCelsiusLogicalFunction(arguments.children[0]);
}

}
