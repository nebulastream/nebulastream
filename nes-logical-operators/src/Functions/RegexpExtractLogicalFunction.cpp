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

#include <Functions/RegexpExtractLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <regex>
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
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

namespace
{
void expectType(const LogicalFunction& argument, const DataType::Type expected, const std::string_view position)
{
    if (not argument.getDataType().isType(expected))
    {
        throw DifferentFieldTypeExpected(
            "REGEXP_EXTRACT expects a {} as its {} argument, but got {}",
            magic_enum::enum_name(expected),
            position,
            argument.getDataType());
    }
}

[[maybe_unused]] void expectInteger(const LogicalFunction& argument, const std::string_view position)
{
    if (not argument.getDataType().isInteger())
    {
        throw DifferentFieldTypeExpected(
            "REGEXP_EXTRACT expects an integer as its {} argument, but got {}", position, argument.getDataType());
    }
}

[[maybe_unused]] bool anyNullable(const std::vector<LogicalFunction>& children)
{
    return std::ranges::any_of(children, [](const auto& child) { return child.getDataType().nullable; });
}

DataType varsizedResult(const bool nullable)
{
    auto result = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    result.nullable = nullable;
    return result;
}
}

RegexpExtractLogicalFunction::RegexpExtractLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), children(std::move(children))
{
}

bool RegexpExtractLogicalFunction::operator==(const RegexpExtractLogicalFunction& rhs) const
{
    return children == rhs.children;
}

std::string RegexpExtractLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format(
        "REGEXP_EXTRACT({})",
        fmt::join(children | std::views::transform([verbosity](const auto& child) { return child.explain(verbosity); }), ", "));
}

DataType RegexpExtractLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction RegexpExtractLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.children = children | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    expectType(copy.children[0], DataType::Type::VARSIZED, "first");
    expectType(copy.children[1], DataType::Type::VARSIZED, "second");
    if (copy.children.size() == 3)
    {
        expectInteger(copy.children[2], "third");
    }
    if (const auto constantRegex = copy.children[1].tryGetAs<ConstantValueLogicalFunction>())
    {
        const auto pattern = constantRegex.value()->getConstantValue();
        try
        {
            const std::regex compiled{pattern, std::regex::ECMAScript};
        }
        catch (const std::regex_error& error)
        {
            throw InvalidLiteral("REGEXP_EXTRACT got an invalid regular expression '{}': {}", pattern, error.what());
        }
    }
    copy.dataType = varsizedResult(true);
    return copy;
}

std::vector<LogicalFunction> RegexpExtractLogicalFunction::getChildren() const
{
    return children;
}

RegexpExtractLogicalFunction RegexpExtractLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(
        children.size() == this->children.size(),
        "RegexpExtractLogicalFunction requires {} children, but got {}",
        this->children.size(),
        children.size());
    auto copy = *this;
    copy.children = children;
    return copy;
}

std::string_view RegexpExtractLogicalFunction::getType() const
{
    return NAME;
}

Reflected
Reflector<RegexpExtractLogicalFunction>::operator()(const RegexpExtractLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedRegexpExtractLogicalFunction{.children = function.children});
}

RegexpExtractLogicalFunction
Unreflector<RegexpExtractLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [children] = context.unreflect<detail::ReflectedRegexpExtractLogicalFunction>(reflected);
    return RegexpExtractLogicalFunction{std::move(children)};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
LogicalFunctionRegistryReturnType RegexpExtractLogicalFunction::createREGEXP_EXTRACT(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() < 2 or arguments.children.size() > 3)
    {
        throw CannotDeserialize("REGEXP_EXTRACT requires 2 or 3 arguments, but got {}", arguments.children.size());
    }
    return RegexpExtractLogicalFunction(std::move(arguments.children));
}

}
