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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// `HTTP_CALL(expression, 'url' [, maxConcurrency [, timeoutSeconds]])` -- POSTs the expression's
/// value to that URL and produces the response body as text.
///
/// It is the demonstration that the asynchronous operator framework is not about language models:
/// any operator that waits on an HTTP service can run on it. It is also the only such operator that
/// needs no model server to be exercised, which is what makes an end-to-end test of the framework
/// possible without one.
///
/// **This function is never evaluated.** There is deliberately no physical function. A plan rule
/// (`HttpCallAsyncRule`) recognises the projection that contains it and marks that projection for
/// asynchronous execution; `AsyncOperatorSplitter` then replaces the whole projection with a
/// handoff sink and an asynchronous source, so the operator is gone before lowering. Writing it
/// anywhere the rule cannot handle -- a WHERE clause, an aggregation -- is therefore rejected by
/// that rule rather than silently producing nothing.
///
/// The URL and the two numbers are read out of constant arguments once, here, and kept as plain
/// values rather than as child expressions: the rule needs them, and an optimizer rule that
/// rewrote the children must not be able to take them away.
class HttpCallLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "HTTP_CALL";

    HttpCallLogicalFunction(LogicalFunction input, std::string url, uint64_t maxConcurrency, uint64_t timeoutSeconds);

    /// Where the request goes. Read by `HttpCallAsyncRule`.
    [[nodiscard]] const std::string& getUrl() const { return url; }

    [[nodiscard]] uint64_t getMaxConcurrency() const { return maxConcurrency; }

    [[nodiscard]] uint64_t getTimeoutSeconds() const { return timeoutSeconds; }

    /// The expression whose value is sent. In the form the rule supports this is a field access.
    [[nodiscard]] const LogicalFunction& getInput() const { return input; }

    [[nodiscard]] bool operator==(const HttpCallLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] HttpCallLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static LogicalFunctionRegistryReturnType createHTTP_CALL(LogicalFunctionRegistryArguments arguments);

    /// Used when a query does not say otherwise. One call at a time would make the framework
    /// pointless to demonstrate, and the default is the same order of magnitude a small service
    /// tolerates.
    static constexpr uint64_t DefaultMaxConcurrency = 8;
    static constexpr uint64_t DefaultTimeoutSeconds = 30;

private:
    DataType dataType;
    LogicalFunction input;
    std::string url;
    uint64_t maxConcurrency = DefaultMaxConcurrency;
    uint64_t timeoutSeconds = DefaultTimeoutSeconds;

    friend Reflector<HttpCallLogicalFunction>;
};

template <>
struct Reflector<HttpCallLogicalFunction>
{
    Reflected operator()(const HttpCallLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<HttpCallLogicalFunction>
{
    HttpCallLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<HttpCallLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedHttpCallLogicalFunction
{
    LogicalFunction input;
    std::string url;
    uint64_t maxConcurrency;
    uint64_t timeoutSeconds;
};
}

FMT_OSTREAM(NES::HttpCallLogicalFunction);
