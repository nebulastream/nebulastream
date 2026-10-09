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

#include <LookbackLogicalFunction.hpp>

#include <array>
#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

#include <LookbackConnections.hpp>
#include <LookbackTemplate.hpp>
#include <ToStringLogicalFunction.hpp>

namespace NES
{

namespace
{

/// Names of the three configuration arguments, for error messages that point at the right one.
constexpr std::array<std::string_view, LookbackLogicalFunction::CONFIGURATION_ARGUMENTS> ARGUMENT_NAMES{
    "connection name", "lookback SQL", "row template"};

std::string extractConstant(const LogicalFunction& function, size_t index)
{
    auto constant = function.tryGetAs<ConstantValueLogicalFunction>();
    if (!constant)
    {
        throw CannotInferSchema("LOOKBACK argument {} ({}) must be a constant VARSIZED literal", index + 1, ARGUMENT_NAMES.at(index));
    }
    if (constant->get().getDataType().type != DataType::Type::VARSIZED)
    {
        throw CannotInferSchema(
            "LOOKBACK argument {} ({}) must be a constant VARSIZED literal, but got data type {}",
            index + 1,
            ARGUMENT_NAMES.at(index),
            constant->get().getDataType());
    }
    return constant->get().getConstantValue();
}

}

LookbackLogicalFunction::LookbackLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), children(std::move(children))
{
}

bool LookbackLogicalFunction::operator==(const LookbackLogicalFunction& rhs) const
{
    return children == rhs.children;
}

DataType LookbackLogicalFunction::getDataType() const
{
    return dataType;
}

LookbackLogicalFunction LookbackLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static) — satisfies LogicalFunctionConcept, cannot be static
std::string_view LookbackLogicalFunction::getType() const
{
    return NAME;
}

std::vector<LogicalFunction> LookbackLogicalFunction::getChildren() const
{
    return children;
}

LookbackLogicalFunction LookbackLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::string LookbackLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto explained = children | std::views::transform([verbosity](const auto& child) { return child.explain(verbosity); })
        | std::ranges::to<std::vector>();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("LOOKBACK({} : {})", fmt::join(explained, ", "), dataType);
    }
    return fmt::format("LOOKBACK({})", fmt::join(explained, ", "));
}

LogicalFunction LookbackLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    if (children.size() < CONFIGURATION_ARGUMENTS)
    {
        throw CannotInferSchema(
            "LOOKBACK requires at least {} arguments (connection name, lookback SQL, row template), but got {}",
            CONFIGURATION_ARGUMENTS,
            children.size());
    }

    /// Infer first: a bare 'literal' arrives as an UNDEFINED constant that still carries its
    /// quotes, and only becomes a VARSIZED constant with the quotes stripped during inference.
    const auto inferredChildren = children
        | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();

    /// Resolve the connection here, at submission, rather than only when the function is lowered.
    /// Lowering happens per placed sub-plan: in a distributed plan the projection carrying the
    /// LOOKBACK can land on a different node than the selection, and a lowering failure there
    /// leaves the upstream node's network sink retrying a channel whose receiver never came up —
    /// the query hangs instead of failing. Failing at submission also tells the author immediately.
    /// The consequence is that the node a query is submitted to needs the connection config too.
    (void)Lookback::LookbackConnectionRegistry::instance().get(extractConstant(inferredChildren[0], 0));
    const auto sql = extractConstant(inferredChildren[1], 1);
    const auto rowTemplateText = extractConstant(inferredChildren[2], 2);

    /// Parse the template here so a malformed one is rejected at query submission rather than on
    /// the first alert, hours later.
    (void)Lookback::RowTemplate::parse(rowTemplateText);

    const auto markers = Lookback::countParameterMarkers(sql);
    const auto suppliedParameters = inferredChildren.size() - CONFIGURATION_ARGUMENTS;
    if (markers != suppliedParameters)
    {
        throw CannotInferSchema(
            "LOOKBACK SQL has {} '?' parameter marker(s) but {} parameter argument(s) were provided", markers, suppliedParameters);
    }

    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(inferredChildren.size());
    for (size_t index = 0; index < CONFIGURATION_ARGUMENTS; ++index)
    {
        newChildren.emplace_back(inferredChildren[index]);
    }

    /// Parameters reach the driver as text, so wrap anything that is not already VARSIZED — the
    /// same trick FMT uses to keep its physical side dealing in one representation.
    for (size_t index = CONFIGURATION_ARGUMENTS; index < inferredChildren.size(); ++index)
    {
        const auto& argument = inferredChildren[index];
        if (argument.getDataType().type == DataType::Type::VARSIZED)
        {
            newChildren.emplace_back(argument);
        }
        else
        {
            newChildren.emplace_back(ToStringLogicalFunction(argument).withInferredDataType(schema));
        }
    }

    /// Deliberately not nullable: "nothing found" is the empty string, so downstream FMT and
    /// comparisons never have to reason about a NULL that only means "the lookback was quiet".
    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = false;
    return withDataType(newDataType).withChildren(newChildren);
}

Reflected Reflector<LookbackLogicalFunction>::operator()(const LookbackLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedLookbackLogicalFunction{.children = function.children});
}

LookbackLogicalFunction Unreflector<LookbackLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto result = context.unreflect<detail::ReflectedLookbackLogicalFunction>(reflected);
    return LookbackLogicalFunction(std::move(result.children));
}

LogicalFunctionRegistryReturnType LookbackLogicalFunction::createLOOKBACK(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() < CONFIGURATION_ARGUMENTS)
    {
        throw CannotDeserialize(
            "LOOKBACK requires at least {} arguments (connection name, lookback SQL, row template), but got {}",
            CONFIGURATION_ARGUMENTS,
            arguments.children.size());
    }
    return LookbackLogicalFunction(std::move(arguments.children));
}

}
