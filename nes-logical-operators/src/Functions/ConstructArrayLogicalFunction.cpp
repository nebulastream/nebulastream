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

#include <Functions/ConstructArrayLogicalFunction.hpp>

#include <DataTypes/DataType.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Reflection.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
ConstructArrayLogicalFunction::ConstructArrayLogicalFunction(std::vector<LogicalFunction> children) : children(std::move(children))
{
    PRECONDITION(not this->children.empty(), "We currently do not support empty arrays");
}

bool ConstructArrayLogicalFunction::operator==(const ConstructArrayLogicalFunction& rhs) const
{
    return dataType == rhs.dataType && children == rhs.children;
}

DataType ConstructArrayLogicalFunction::getDataType() const
{
    return dataType;
}

ConstructArrayLogicalFunction ConstructArrayLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ConstructArrayLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto newChildren = children | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    /// Infer data type of array elements
    DataType inferredType = newChildren.at(0).getDataType();
    inferredType.nullable = false;
    for (size_t i = 1; i < newChildren.size(); ++i)
    {
        auto childType = newChildren.at(i).getDataType();
        childType.nullable = false;
        const std::optional<DataType> joinType = childType.join(inferredType);
        if (joinType.has_value())
        {
            inferredType = *joinType;
        }
        else
        {
            throw CannotInferStamp("Could not join types of array elements: Join failed for {} and {}", inferredType, childType);
        }
    }

    if (inferredType.type == DataType::Type::UNDEFINED)
    {
        throw UnknownDataType("Arrays are not allowed to have UNDEFINED as element type.");
    }

    const DataType arrayType{
        DataType::Type::FIXEDSIZED, DataType::NULLABLE::NOT_NULLABLE, inferredType, static_cast<uint32_t>(newChildren.size())};

    if (not arrayType.isValid())
    {
        throw NestedVariableSizedType("Nesting of variable-sized data types is currently not supported.");
    }

    /// Apply a cast function to each child which does not match the type
    auto castedChildren = newChildren
        | std::views::transform(
                              [inferredType](const auto& child)
                              {
                                  auto childType = child.getDataType();
                                  childType.nullable = false;
                                  return childType == inferredType ? child : CastToTypeLogicalFunction{inferredType, child};
                              })
        | std::ranges::to<std::vector>();

    return ConstructArrayLogicalFunction{std::move(castedChildren)}.withDataType(arrayType);
}

std::vector<LogicalFunction> ConstructArrayLogicalFunction::getChildren() const
{
    return children;
}

std::string_view ConstructArrayLogicalFunction::getType() const
{
    return NAME;
}

ConstructArrayLogicalFunction ConstructArrayLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::string ConstructArrayLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto childStrings
        = children | std::views::transform([&](const auto& c) { return c.explain(verbosity); }) | std::ranges::to<std::vector>();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ConstructArray({}) : {})", fmt::join(childStrings, ", "), dataType);
    }
    return fmt::format("Array({})", fmt::join(childStrings, ", "));
}

Reflected
Reflector<ConstructArrayLogicalFunction>::operator()(const ConstructArrayLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedConstructArrayLogicalFunction{.children = function.getChildren()});
}

ConstructArrayLogicalFunction
Unreflector<ConstructArrayLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [children] = context.unreflect<detail::ReflectedConstructArrayLogicalFunction>(reflected);
    return ConstructArrayLogicalFunction{std::move(children)};
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType ConstructArrayLogicalFunction::createConstructArray(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "ConstructArrayLogicalFunction is built directly via parser or via reflection, not the registry");
    std::unreachable();
}

/// NOLINTEND(performance-unnecessary-value-param)
}
