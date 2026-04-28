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

#include "FmtLogicalFunction.hpp"

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

#include "ToStringLogicalFunction.hpp"

namespace NES
{

namespace
{
std::string extractFormatString(const LogicalFunction& fn)
{
    auto constant = fn.tryGetAs<ConstantValueLogicalFunction>();
    if (!constant)
    {
        throw CannotInferSchema("FMT requires a constant VARSIZED literal as its first argument");
    }
    if (constant->get().getDataType().type != DataType::Type::VARSIZED)
    {
        throw CannotInferSchema(
            "FMT requires a constant VARSIZED literal as its first argument, but got data type {}", constant->get().getDataType());
    }
    return constant->get().getConstantValue();
}

/// Counts top-level `{}` placeholders. Bracket pairs that are not balanced are
/// rejected here so the per-row physical execution never has to error out.
size_t countPlaceholders(std::string_view fmt)
{
    size_t count = 0;
    bool inArgument = false;
    for (auto c : fmt)
    {
        if (!inArgument && c == '{')
        {
            inArgument = true;
        }
        else if (inArgument && c == '}')
        {
            inArgument = false;
            ++count;
        }
    }
    if (inArgument)
    {
        throw CannotInferSchema("FMT format string has an unmatched '{{'");
    }
    return count;
}
}

FmtLogicalFunction::FmtLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), children(std::move(children))
{
}

bool FmtLogicalFunction::operator==(const FmtLogicalFunction& rhs) const
{
    return children == rhs.children;
}

DataType FmtLogicalFunction::getDataType() const
{
    return dataType;
}

FmtLogicalFunction FmtLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

std::string_view FmtLogicalFunction::getType() const
{
    return NAME;
}

std::vector<LogicalFunction> FmtLogicalFunction::getChildren() const
{
    return children;
}

FmtLogicalFunction FmtLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::string FmtLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto explained
        = children | std::views::transform([verbosity](const auto& c) { return c.explain(verbosity); }) | std::ranges::to<std::vector>();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("FMT({} : {})", fmt::join(explained, ", "), dataType);
    }
    return fmt::format("FMT({})", fmt::join(explained, ", "));
}

LogicalFunction FmtLogicalFunction::withInferredDataType(const Schema& schema) const
{
    if (children.empty())
    {
        throw CannotInferSchema("FMT requires at least the format string as its first argument");
    }

    const auto inferredChildren
        = children | std::views::transform([&schema](const auto& c) { return c.withInferredDataType(schema); }) | std::ranges::to<std::vector>();

    const auto formatString = extractFormatString(inferredChildren.front());
    const auto placeholders = countPlaceholders(formatString);
    if (placeholders + 1 != inferredChildren.size())
    {
        throw CannotInferSchema(
            "FMT format string has {} placeholders but {} argument(s) were provided", placeholders, inferredChildren.size() - 1);
    }

    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(inferredChildren.size());
    newChildren.emplace_back(inferredChildren.front());

    for (size_t argIdx = 1; argIdx < inferredChildren.size(); ++argIdx)
    {
        const auto& arg = inferredChildren[argIdx];
        if (arg.getDataType().type == DataType::Type::VARSIZED)
        {
            newChildren.emplace_back(arg);
        }
        else
        {
            newChildren.emplace_back(ToStringLogicalFunction(arg).withInferredDataType(schema));
        }
    }

    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable
        = std::ranges::any_of(inferredChildren, [](const auto& c) { return c.getDataType().nullable; });
    return withDataType(newDataType).withChildren(newChildren);
}

Reflected Reflector<FmtLogicalFunction>::operator()(const FmtLogicalFunction& function) const
{
    return reflect(detail::ReflectedFmtLogicalFunction{.children = function.children});
}

FmtLogicalFunction Unreflector<FmtLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto result = unreflect<detail::ReflectedFmtLogicalFunction>(reflected);
    return FmtLogicalFunction(std::move(result.children));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterFMTLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<FmtLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("FMT requires at least the format string as its first argument");
    }
    return FmtLogicalFunction(std::move(arguments.children));
}

}
