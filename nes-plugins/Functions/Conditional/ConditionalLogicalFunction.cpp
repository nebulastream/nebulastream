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

#include <iterator>
#include <ranges>
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
/// Groups the leading elements of the flat child list into (when, then) pairs, leaving out the
/// trailing default result.
std::vector<WhenThenLogicalFunction> groupWhenThens(const std::vector<LogicalFunction>& children)
{
    INVARIANT(
        children.size() >= 3 and children.size() % 2 == 1,
        "ConditionalLogicalFunction requires an odd number of children >= 3 (when/then pairs plus a default), but got {}",
        children.size());

    std::vector<WhenThenLogicalFunction> whenThens;
    whenThens.reserve(children.size() / 2);
    for (auto when = children.begin(); when != std::prev(children.end()); when += 2)
    {
        whenThens.push_back(WhenThenLogicalFunction{.when = *when, .then = *std::next(when)});
    }
    return whenThens;
}
}

/// The result type stays untouched here; it is inferred once the input schema is known.
ConditionalLogicalFunction::ConditionalLogicalFunction(std::vector<WhenThenLogicalFunction> whenThens, LogicalFunction elseCase)
    : whenThens(std::move(whenThens)), elseCase(std::move(elseCase))
{
    INVARIANT(not this->whenThens.empty(), "ConditionalLogicalFunction requires at least one when/then pair");
}

ConditionalLogicalFunction::ConditionalLogicalFunction(const std::vector<LogicalFunction>& children)
    : whenThens(groupWhenThens(children)), elseCase(children.back())
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
    children.push_back(elseCase);
    return children;
}

ConditionalLogicalFunction ConditionalLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.whenThens = groupWhenThens(children);
    copy.elseCase = children.back();
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
    /// The default result carries the type of the whole expression.
    const auto inferredElseCase = elseCase.withInferredDataType(schema);
    auto resultType = inferredElseCase.getDataType();

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
                "CASE WHEN results must all share the default result's type {}, but got {}", resultType, inferredThen.getDataType());
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
    if (children.size() < 3 or children.size() % 2 == 0)
    {
        throw CannotDeserialize("ConditionalLogicalFunction requires an odd number of children >= 3, but got {}", children.size());
    }
    return ConditionalLogicalFunction{children};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterConditionalLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    /// The argument count comes straight from the user's query, so a bad count is reported rather than asserted.
    if (arguments.children.size() < 3 or arguments.children.size() % 2 == 0)
    {
        throw CannotDeserialize(
            "Conditional requires an odd number of arguments >= 3 (condition/result pairs plus a default), but got {}",
            arguments.children.size());
    }
    return ConditionalLogicalFunction{arguments.children};
}
}
