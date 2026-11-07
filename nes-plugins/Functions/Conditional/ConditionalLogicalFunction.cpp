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

#include <Functions/ConditionalLogicalFunction.hpp>

#include <cstddef>
#include <ranges>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
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

namespace NES
{
namespace
{
/// A child list is a sequence of (when, then) pairs, optionally followed by the default result.
bool isValidChildCount(const size_t numberOfChildren)
{
    return numberOfChildren >= 2;
}

/// Groups the leading elements of the flat child list into (when, then) pairs, leaving out the
/// trailing default result if there is one.
std::vector<WhenThenLogicalFunction> groupWhenThens(const std::vector<LogicalFunction>& children)
{
    INVARIANT(
        isValidChildCount(children.size()),
        "ConditionalLogicalFunction requires at least one when/then pair, but got {} children",
        children.size());

    std::vector<WhenThenLogicalFunction> whenThens;
    whenThens.reserve(children.size() / 2);
    for (size_t index = 0; index + 1 < children.size(); index += 2)
    {
        whenThens.push_back(WhenThenLogicalFunction{.when = children[index], .then = children[index + 1]});
    }
    return whenThens;
}

std::optional<LogicalFunction> defaultResult(const std::vector<LogicalFunction>& children)
{
    return children.size() % 2 == 1 ? std::optional{children.back()} : std::nullopt;
}
}

/// The result type stays untouched here; it is inferred once the input schema is known.
ConditionalLogicalFunction::ConditionalLogicalFunction(std::vector<WhenThenLogicalFunction> whenThens, std::optional<LogicalFunction> elseCase)
    : whenThens(std::move(whenThens)), elseCase(std::move(elseCase))
{
    INVARIANT(not this->whenThens.empty(), "ConditionalLogicalFunction requires at least one when/then pair");
}

ConditionalLogicalFunction::ConditionalLogicalFunction(const std::vector<LogicalFunction>& children)
    : whenThens(groupWhenThens(children)), elseCase(defaultResult(children))
{
}

DataType ConditionalLogicalFunction::getDataType() const
{
    return dataType;
}

std::vector<LogicalFunction> ConditionalLogicalFunction::getChildren() const
{
    std::vector<LogicalFunction> children;
    children.reserve((whenThens.size() * 2) + 1);
    for (const auto& [when, then] : whenThens)
    {
        children.push_back(when);
        children.push_back(then);
    }
    if (elseCase.has_value())
    {
        children.push_back(*elseCase);
    }
    return children;
}

ConditionalLogicalFunction ConditionalLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.whenThens = groupWhenThens(children);
    copy.elseCase = defaultResult(children);
    return copy;
}

std::string_view ConditionalLogicalFunction::getType() const
{
    return NAME;
}

bool ConditionalLogicalFunction::operator==(const ConditionalLogicalFunction& rhs) const
{
    return whenThens == rhs.whenThens and elseCase == rhs.elseCase;
}

std::string ConditionalLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    /// Rendered in function-call form because the SQL parser does not support the CASE WHEN syntax yet.
    const auto children = getChildren();
    const auto explained = children | std::views::transform([verbosity](const LogicalFunction& child) { return child.explain(verbosity); })
        | std::ranges::to<std::vector>();
    return fmt::format("{}({})", NAME, fmt::join(explained, ", "));
}

LogicalFunction ConditionalLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    /// The default result carries the type of the whole expression. Without one, the default is NULL, as in SQL: the
    /// expression takes the type of the first result and is nullable.
    const auto inferredElseCase
        = elseCase.transform([&schema](const LogicalFunction& function) { return function.withInferredDataType(schema); });
    auto resultType = inferredElseCase.has_value()
        ? inferredElseCase->getDataType()
        : DataTypeProvider::provideDataType(whenThens.front().then.withInferredDataType(schema).getDataType().type, DataType::NULLABLE::IS_NULLABLE);

    std::vector<WhenThenLogicalFunction> inferredWhenThens;
    inferredWhenThens.reserve(whenThens.size());
    for (const auto& [when, then] : whenThens)
    {
        auto inferredWhen = when.withInferredDataType(schema);
        auto inferredThen = then.withInferredDataType(schema);
        if (not inferredWhen.getDataType().isType(DataType::Type::BOOLEAN))
        {
            throw DifferentFieldTypeExpected("CASE WHEN condition must be BOOLEAN, but got {}", inferredWhen.getDataType());
        }
        if (not inferredThen.getDataType().isType(resultType.type))
        {
            throw DifferentFieldTypeExpected(
                "CASE WHEN results must all share one type {}, but got {}", resultType, inferredThen.getDataType());
        }
        /// A branch that can produce a null makes the whole expression nullable, as in SQL.
        resultType = DataTypeProvider::provideDataType(resultType.type, resultType.joinNullable(inferredThen.getDataType()));
        inferredWhenThens.push_back(WhenThenLogicalFunction{.when = std::move(inferredWhen), .then = std::move(inferredThen)});
    }

    auto inferred = ConditionalLogicalFunction{std::move(inferredWhenThens), inferredElseCase};
    inferred.dataType = resultType;
    return inferred;
}

Reflected
Reflector<ConditionalLogicalFunction>::operator()(const ConditionalLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedConditionalLogicalFunction{.children = function.getChildren()});
}

ConditionalLogicalFunction
Unreflector<ConditionalLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    const auto [children] = context.unreflect<detail::ReflectedConditionalLogicalFunction>(reflected);
    /// The shape of a serialized plan is not trusted, so a bad child count is reported rather than asserted.
    if (not isValidChildCount(children.size()))
    {
        throw CannotDeserialize("ConditionalLogicalFunction requires at least one when/then pair, but got {} children", children.size());
    }
    return ConditionalLogicalFunction{children};
}

LogicalFunctionRegistryReturnType ConditionalLogicalFunction::createConditional(LogicalFunctionRegistryArguments arguments)
{
    /// The argument count comes straight from the user's query, so a bad count is reported rather than asserted.
    if (not isValidChildCount(arguments.children.size()))
    {
        throw CannotDeserialize(
            "Conditional requires condition/result pairs plus an optional default, but got {} arguments", arguments.children.size());
    }
    return ConditionalLogicalFunction{arguments.children};
}
}
