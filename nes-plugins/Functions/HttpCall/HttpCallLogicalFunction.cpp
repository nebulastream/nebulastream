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
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

HttpCallLogicalFunction::HttpCallLogicalFunction(
    LogicalFunction body, LogicalFunction url, std::optional<LogicalFunction> timeoutMillis, std::optional<LogicalFunction> retries)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
    , body(std::move(body))
    , url(std::move(url))
    , timeoutMillis(std::move(timeoutMillis))
    , retries(std::move(retries))
{
    PRECONDITION(!this->retries || this->timeoutMillis.has_value(), "HTTP_CALL retries must follow the timeout argument");
}

bool HttpCallLogicalFunction::operator==(const HttpCallLogicalFunction& rhs) const
{
    return body == rhs.body and url == rhs.url and timeoutMillis == rhs.timeoutMillis and retries == rhs.retries;
}

DataType HttpCallLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction HttpCallLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.body = body.withInferredDataType(schema);
    copy.url = url.withInferredDataType(schema);
    for (const auto& argument : {copy.body, copy.url})
    {
        if (not argument.getDataType().isType(DataType::Type::VARSIZED) or argument.getDataType().nullable)
        {
            throw DifferentFieldTypeExpected("HTTP_CALL expects non-nullable VARSIZED body and URL, but got {}", argument);
        }
    }
    if (timeoutMillis)
    {
        copy.timeoutMillis = timeoutMillis->withInferredDataType(schema);
        if (not copy.timeoutMillis->getDataType().isInteger() or copy.timeoutMillis->getDataType().nullable)
        {
            throw DifferentFieldTypeExpected(
                "HTTP_CALL expects a non-nullable integer timeout in milliseconds, but got {}", *copy.timeoutMillis);
        }
    }
    if (retries)
    {
        copy.retries = retries->withInferredDataType(schema);
        if (not copy.retries->getDataType().isInteger() or copy.retries->getDataType().nullable)
        {
            throw DifferentFieldTypeExpected("HTTP_CALL expects a non-nullable integer retry count, but got {}", *copy.retries);
        }
    }
    return copy;
}

std::vector<LogicalFunction> HttpCallLogicalFunction::getChildren() const
{
    if (retries)
    {
        INVARIANT(timeoutMillis.has_value(), "HTTP_CALL retries must follow the timeout argument");
        return {body, url, *timeoutMillis, *retries};
    }
    if (timeoutMillis)
    {
        return {body, url, *timeoutMillis};
    }
    return {body, url};
}

HttpCallLogicalFunction HttpCallLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() >= 2 && children.size() <= 4, "HTTP_CALL takes 2 to 4 children");
    auto copy = *this;
    copy.body = children.at(0);
    copy.url = children.at(1);
    copy.timeoutMillis = children.size() > 2 ? std::optional{children.at(2)} : std::nullopt;
    copy.retries = children.size() > 3 ? std::optional{children.at(3)} : std::nullopt;
    return copy;
}

std::string_view HttpCallLogicalFunction::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

std::string HttpCallLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (retries)
    {
        INVARIANT(timeoutMillis.has_value(), "HTTP_CALL retries must follow the timeout argument");
        return fmt::format(
            "HTTP_CALL({}, {}, {}, {})",
            body.explain(verbosity),
            url.explain(verbosity),
            timeoutMillis->explain(verbosity),
            retries->explain(verbosity));
    }
    if (timeoutMillis)
    {
        return fmt::format("HTTP_CALL({}, {}, {})", body.explain(verbosity), url.explain(verbosity), timeoutMillis->explain(verbosity));
    }
    return fmt::format("HTTP_CALL({}, {})", body.explain(verbosity), url.explain(verbosity));
}

Reflected Reflector<HttpCallLogicalFunction>::operator()(const HttpCallLogicalFunction& function, const ReflectionContext& context) const
{
    const auto children = function.getChildren();
    return context.reflect(
        detail::ReflectedHttpCallLogicalFunction{
            .body = children.at(0),
            .url = children.at(1),
            .timeoutMillis = children.size() > 2 ? std::optional{children.at(2)} : std::nullopt,
            .retries = children.size() > 3 ? std::optional{children.at(3)} : std::nullopt});
}

HttpCallLogicalFunction Unreflector<HttpCallLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [body, url, timeoutMillis, retries] = context.unreflect<detail::ReflectedHttpCallLogicalFunction>(reflected);
    return HttpCallLogicalFunction{std::move(body), std::move(url), std::move(timeoutMillis), std::move(retries)};
}

LogicalFunctionRegistryReturnType HttpCallLogicalFunction::createHTTP_CALL(LogicalFunctionRegistryArguments arguments)
{
    /// The argument count comes straight from the user's query, so a bad count is reported rather than asserted.
    if (arguments.children.size() < 2 or arguments.children.size() > 4)
    {
        throw InvalidQuerySyntax(
            "HTTP_CALL expects 2 to 4 arguments (body, url[, timeout_ms[, retries]]), but got {}", arguments.children.size());
    }
    return HttpCallLogicalFunction{
        arguments.children.at(0),
        arguments.children.at(1),
        arguments.children.size() > 2 ? std::optional{arguments.children.at(2)} : std::nullopt,
        arguments.children.size() > 3 ? std::optional{arguments.children.at(3)} : std::nullopt};
}

}
