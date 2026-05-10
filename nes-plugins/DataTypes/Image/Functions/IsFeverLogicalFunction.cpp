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

#include <IsFeverLogicalFunction.hpp>

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

IsFeverLogicalFunction::IsFeverLogicalFunction(const LogicalFunction& child)
    : dataType(DataType::Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE), child(child)
{
}

DataType IsFeverLogicalFunction::getDataType() const
{
    return dataType;
}

IsFeverLogicalFunction IsFeverLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction IsFeverLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChildren = getChildren() | std::views::transform([&schema](auto& c) { return c.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    INVARIANT(newChildren.size() == 1, "IsFeverLogicalFunction expects exactly one child function but has {}", newChildren.size());
    /// Same layout gate as to_celsius — any STRUCT with a single
    /// FIXEDSIZED<UINT16> 'pixels' field is accepted.
    const auto childType = newChildren[0].getDataType();
    if (childType.type != DataType::Type::STRUCT || childType.fields.size() != 1
        || childType.fields[0].first != "pixels"
        || childType.fields[0].second.type != DataType::Type::FIXEDSIZED
        || childType.fields[0].second.elementType != DataType::Type::UINT16)
    {
        throw DifferentFieldTypeExpected(
            "is_fever expects a STRUCT with a single FIXEDSIZED<UINT16> 'pixels' field, got {}", childType);
    }
    const auto nullable = childType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    auto newDataType = DataType{DataType::Type::BOOLEAN, nullable};
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> IsFeverLogicalFunction::getChildren() const
{
    return {child};
}

IsFeverLogicalFunction IsFeverLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "IsFeverLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view IsFeverLogicalFunction::getType() const
{
    return NAME;
}

bool IsFeverLogicalFunction::operator==(const IsFeverLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string IsFeverLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("IsFeverLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("is_fever({})", child.explain(verbosity));
}

Reflected Reflector<IsFeverLogicalFunction>::operator()(const IsFeverLogicalFunction& function) const
{
    return reflect(detail::ReflectedIsFeverLogicalFunction{.child = function.child});
}

IsFeverLogicalFunction Unreflector<IsFeverLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedIsFeverLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("IsFeverLogicalFunction is missing its child");
    }
    return IsFeverLogicalFunction(child.value());
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterIS_FEVERLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<IsFeverLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("IsFeverLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return IsFeverLogicalFunction(arguments.children[0]);
}

}
