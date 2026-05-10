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

#include <Functions/ConstructStructLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

ConstructStructLogicalFunction::ConstructStructLogicalFunction(DataType structType, std::vector<LogicalFunction> children)
    : structType(std::move(structType)), children(std::move(children))
{
}

bool ConstructStructLogicalFunction::operator==(const ConstructStructLogicalFunction& rhs) const
{
    return structType == rhs.structType && children == rhs.children;
}

DataType ConstructStructLogicalFunction::getDataType() const
{
    return structType;
}

ConstructStructLogicalFunction ConstructStructLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.structType = dataType;
    return copy;
}

LogicalFunction ConstructStructLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChildren = children | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    if (structType.type != DataType::Type::STRUCT)
    {
        throw DifferentFieldTypeExpected("ConstructStruct expects a STRUCT target type, got {}", structType);
    }
    if (newChildren.size() != structType.fields.size())
    {
        throw DifferentFieldTypeExpected(
            "Struct constructor for '{}' expects {} arguments, got {}", structType.structName, structType.fields.size(), newChildren.size());
    }
    /// Each child must produce the field's declared type (ignoring nullability — a
    /// non-null source feeding a nullable field is fine; a null source feeding a
    /// non-null field is caught later when the value is materialised).
    for (size_t i = 0; i < newChildren.size(); ++i)
    {
        const auto& [fieldName, fieldType] = structType.fields[i];
        auto childType = newChildren[i].getDataType();
        childType.nullable = fieldType.nullable;
        if (childType != fieldType)
        {
            throw DifferentFieldTypeExpected(
                "Struct constructor for '{}': field '{}' expects {}, got {}",
                structType.structName,
                fieldName,
                fieldType,
                newChildren[i].getDataType());
        }
    }

    auto resolved = structType;
    resolved.nullable = structType.nullable || std::ranges::any_of(newChildren, [](const auto& c) { return c.getDataType().nullable; });
    return ConstructStructLogicalFunction{resolved, std::move(newChildren)};
}

std::vector<LogicalFunction> ConstructStructLogicalFunction::getChildren() const
{
    return children;
}

ConstructStructLogicalFunction ConstructStructLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    auto copy = *this;
    copy.children = newChildren;
    return copy;
}

std::string_view ConstructStructLogicalFunction::getType() const
{
    return NAME;
}

std::string ConstructStructLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto childStrings = children | std::views::transform([&](const auto& c) { return c.explain(verbosity); }) | std::ranges::to<std::vector>();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ConstructStruct({}({}) : {})", structType.structName, fmt::join(childStrings, ", "), structType);
    }
    return fmt::format("{}({})", structType.structName, fmt::join(childStrings, ", "));
}

Reflected Reflector<ConstructStructLogicalFunction>::operator()(const ConstructStructLogicalFunction& function) const
{
    return reflect(detail::ReflectedConstructStructLogicalFunction{.structType = function.getDataType(), .children = function.getChildren()});
}

ConstructStructLogicalFunction Unreflector<ConstructStructLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [structType, children] = unreflect<detail::ReflectedConstructStructLogicalFunction>(reflected);
    return ConstructStructLogicalFunction{std::move(structType), std::move(children)};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConstructStructLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<ConstructStructLogicalFunction>(arguments.reflected);
    }
    PRECONDITION(false, "ConstructStructLogicalFunction is built directly via parser or via reflection, not the registry");
    std::unreachable();
}

}
