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

#include <Functions/HttpCallLogicalFunction.hpp>

#include <cstddef>
#include <cstdint>
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
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

namespace
{

/// The argument has to be a literal. A URL or a concurrency that depended on the record could not
/// be known when the plan is split, which happens once per query and not once per record.
std::string constantArgument(const LogicalFunction& argument, const size_t position)
{
    if (const auto constant = argument.tryGetAs<ConstantValueLogicalFunction>())
    {
        return constant->get().getConstantValue();
    }
    throw InvalidQuerySyntax("HTTP_CALL argument {} has to be a literal, but was {}", position, argument.explain(ExplainVerbosity::Short));
}

uint64_t numericArgument(const LogicalFunction& argument, const size_t position)
{
    const auto text = constantArgument(argument, position);
    const auto parsed = from_chars<uint64_t>(text);
    if (!parsed.has_value() || parsed.value() == 0)
    {
        throw InvalidQuerySyntax("HTTP_CALL argument {} has to be a positive number, but was '{}'", position, text);
    }
    return parsed.value();
}

}

HttpCallLogicalFunction::HttpCallLogicalFunction(
    LogicalFunction input, std::string url, const uint64_t maxConcurrency, const uint64_t timeoutSeconds)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
    , input(std::move(input))
    , url(std::move(url))
    , maxConcurrency(maxConcurrency)
    , timeoutSeconds(timeoutSeconds)
{
}

bool HttpCallLogicalFunction::operator==(const HttpCallLogicalFunction& rhs) const
{
    return input == rhs.input && url == rhs.url && maxConcurrency == rhs.maxConcurrency && timeoutSeconds == rhs.timeoutSeconds;
}

DataType HttpCallLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction HttpCallLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.input = input.withInferredDataType(schema);
    /// Any input type is fine: the value is rendered as text on the way into the request body, the
    /// same way the asynchronous framework renders a field for any other executor.
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    return copy;
}

std::vector<LogicalFunction> HttpCallLogicalFunction::getChildren() const
{
    return {input};
}

HttpCallLogicalFunction HttpCallLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "HttpCallLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.input = children.front();
    return copy;
}

std::string_view HttpCallLogicalFunction::getType() const
{
    return NAME;
}

std::string HttpCallLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "HTTP_CALL({}, '{}', concurrency={}, timeout={}s)", input.explain(verbosity), url, maxConcurrency, timeoutSeconds);
    }
    return fmt::format("HTTP_CALL({}, '{}')", input.explain(verbosity), url);
}

Reflected Reflector<HttpCallLogicalFunction>::operator()(const HttpCallLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedHttpCallLogicalFunction{
        .input = function.input,
        .url = function.url,
        .maxConcurrency = function.maxConcurrency,
        .timeoutSeconds = function.timeoutSeconds});
}

HttpCallLogicalFunction Unreflector<HttpCallLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [input, url, maxConcurrency, timeoutSeconds] = context.unreflect<detail::ReflectedHttpCallLogicalFunction>(reflected);
    /// A serialized plan is not trusted, so a value that could not have come from the parser is
    /// reported rather than asserted.
    if (url.empty() || maxConcurrency == 0 || timeoutSeconds == 0)
    {
        throw CannotDeserialize(
            "HTTP_CALL needs a URL and positive bounds, but got url '{}', concurrency {}, timeout {}", url, maxConcurrency, timeoutSeconds);
    }
    return HttpCallLogicalFunction{std::move(input), std::move(url), maxConcurrency, timeoutSeconds};
}

/// NOLINTNEXTLINE(readability-identifier-naming, performance-unnecessary-value-param)
LogicalFunctionRegistryReturnType HttpCallLogicalFunction::createHTTP_CALL(LogicalFunctionRegistryArguments arguments)
{
    /// The argument count comes straight from the user's query, so a bad count is reported.
    if (arguments.children.size() < 2 || arguments.children.size() > 4)
    {
        throw InvalidQuerySyntax(
            "HTTP_CALL takes an expression, a URL and optionally a concurrency and a timeout in seconds, so two to four arguments, but "
            "got {}",
            arguments.children.size());
    }

    auto url = constantArgument(arguments.children[1], 2);
    if (url.empty())
    {
        throw InvalidQuerySyntax("HTTP_CALL needs a non-empty URL");
    }
    const auto maxConcurrency = arguments.children.size() > 2 ? numericArgument(arguments.children[2], 3) : DefaultMaxConcurrency;
    const auto timeoutSeconds = arguments.children.size() > 3 ? numericArgument(arguments.children[3], 4) : DefaultTimeoutSeconds;

    return HttpCallLogicalFunction{arguments.children.front(), std::move(url), maxConcurrency, timeoutSeconds};
}

}
