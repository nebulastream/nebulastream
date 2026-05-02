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

#include <Functions/UnboundLogicalFunction.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

UnboundLogicalFunction::UnboundLogicalFunction(std::string operationName, std::vector<LogicalFunction> children)
    : operationName(std::move(operationName)), children(std::move(children))
{
}

bool UnboundLogicalFunction::operator==(const UnboundLogicalFunction& rhs) const
{
    return operationName == rhs.operationName && children == rhs.children;
}

LogicalType UnboundLogicalFunction::getLogicalType() const
{
    return LogicalType{"UNDEFINED", {}, Nullable::IS_NULLABLE};
}

UnboundLogicalFunction UnboundLogicalFunction::withLogicalType(const LogicalType&) const
{
    return *this;
}

LogicalFunction UnboundLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    auto inferredChildren = children
        | std::views::transform([&schema](const auto& child) { return child.withInferredLogicalType(schema); })
        | std::ranges::to<std::vector>();

    std::string mangled = operationName;
    for (const auto& child : inferredChildren)
    {
        mangled += "_";
        mangled += std::string{child.getLogicalType().getName()};
    }

    if (auto resolved = LogicalFunctionProvider::tryProvide(mangled, inferredChildren))
    {
        return resolved->withInferredLogicalType(schema);
    }
    if (auto resolved = LogicalFunctionProvider::tryProvide(operationName, inferredChildren))
    {
        return resolved->withInferredLogicalType(schema);
    }

    auto childTypes = inferredChildren | std::views::transform([](const auto& c) { return std::string{c.getLogicalType().getName()}; })
        | std::ranges::to<std::vector>();
    throw FunctionNotImplemented("no overload of '{}' registered for argument types ({})", operationName, fmt::join(childTypes, ", "));
}

std::vector<LogicalFunction> UnboundLogicalFunction::getChildren() const
{
    return children;
}

UnboundLogicalFunction UnboundLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    return UnboundLogicalFunction{operationName, newChildren};
}

std::string_view UnboundLogicalFunction::getType() const
{
    return NAME;
}

std::string UnboundLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto explained = children | std::views::transform([&](const auto& c) { return c.explain(verbosity); });
    return fmt::format("Unbound[{}]({})", operationName, fmt::join(explained, ", "));
}

Reflected Reflector<UnboundLogicalFunction>::operator()(const UnboundLogicalFunction&) const
{
    throw NotImplemented("UnboundLogicalFunction must be resolved before serialization");
}

}
