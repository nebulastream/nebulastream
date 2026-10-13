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

#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/AsyncLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// `HTTP_CALL(body, url)`: POSTs `body` to `url` and evaluates to the response body.
/// Asynchronous only; there is no Nautilus implementation.
class HttpCallLogicalFunction final : public AsyncLogicalFunction
{
public:
    static constexpr std::string_view NAME = "HttpCall";

    HttpCallLogicalFunction(LogicalFunction body, LogicalFunction url);

    [[nodiscard]] bool operator==(const HttpCallLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] HttpCallLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createHTTP_CALL(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    LogicalFunction body;
    LogicalFunction url;
};

namespace detail
{
struct ReflectedHttpCallLogicalFunction
{
    LogicalFunction body;
    LogicalFunction url;
};
}

template <>
struct Unreflector<HttpCallLogicalFunction>
{
    HttpCallLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<HttpCallLogicalFunction>
{
    Reflected operator()(const HttpCallLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<HttpCallLogicalFunction>);

}

FMT_OSTREAM(NES::HttpCallLogicalFunction);
