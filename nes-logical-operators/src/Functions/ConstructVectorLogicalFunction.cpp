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

#include <Functions/ConstructVectorLogicalFunction.hpp>

#include <cstddef>
#include <optional>
#include <ranges>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{
ConstructVectorLogicalFunction::ConstructVectorLogicalFunction(std::vector<LogicalFunction> children) : children(std::move(children))
{
    PRECONDITION(not this->children.empty(), "We currently do not support empty arrays");
}

bool ConstructVectorLogicalFunction::operator==(const ConstructVectorLogicalFunction& rhs) const
{
    return dataType == rhs.dataType && children == rhs.children;
}

DataType ConstructVectorLogicalFunction::getDataType() const
{
    return dataType;
}

ConstructVectorLogicalFunction ConstructVectorLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction ConstructVectorLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto newChildren = children | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    /// Infer data type of vector elements
    DataType inferredType = newChildren.at(0).getDataType();
    inferredType.nullable = false;
    for (size_t i = 1; i < newChildren.size(); ++i)
    {
        auto childType = newChildren.at(i).getDataType();
        childType.nullable = false;
        if (const std::optional<DataType> joinType = childType.join(inferredType); joinType.has_value())
        {
            inferredType = *joinType;
        }
        else
        {
            throw CannotInferStamp("Could not join types of vector elements: Join failed for {} and {}", inferredType, childType);
        }
    }

    if (inferredType.type == DataType::Type::UNDEFINED)
    {
        throw UnknownDataType("Vectors are not allowed to have UNDEFINED as element type.");
    }

    /// Although we know the number of elements  in the case of constants, the constructor for vector types does not require them.
    /// In the future, we could optimize around this by automatically creating fixedsized arrays for every array constant in the query,
    /// and offering a cast from fixedsized array to vector, if it should be required afterwards.
    const DataType vectorType{DataType::Type::VECTOR, DataType::NULLABLE::NOT_NULLABLE, inferredType};

    if (not vectorType.isValid())
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

    return ConstructVectorLogicalFunction{std::move(castedChildren)}.withDataType(vectorType);
}

std::vector<LogicalFunction> ConstructVectorLogicalFunction::getChildren() const
{
    return children;
}

std::string_view ConstructVectorLogicalFunction::getType() const
{
    return NAME;
}

ConstructVectorLogicalFunction ConstructVectorLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::string ConstructVectorLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto childStrings
        = children | std::views::transform([&](const auto& c) { return c.explain(verbosity); }) | std::ranges::to<std::vector>();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("ConstructVector({}) : {})", fmt::join(childStrings, ", "), dataType);
    }
    return fmt::format("Vector({})", fmt::join(childStrings, ", "));
}

Reflected Reflector<ConstructVectorLogicalFunction>::operator()(
    const ConstructVectorLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedConstructVectorLogicalFunction{.children = function.getChildren()});
}

ConstructVectorLogicalFunction
Unreflector<ConstructVectorLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [children] = context.unreflect<detail::ReflectedConstructVectorLogicalFunction>(reflected);
    return ConstructVectorLogicalFunction{std::move(children)};
}

/// NOLINTBEGIN(performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType ConstructVectorLogicalFunction::createConstructVector(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "ConstructVectorLogicalFunction is built directly via parser or via reflection, not the registry");
    std::unreachable();
}

/// NOLINTEND(performance-unnecessary-value-param)
}
