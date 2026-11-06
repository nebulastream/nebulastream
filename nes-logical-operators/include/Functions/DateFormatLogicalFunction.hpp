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
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// SQL DATE_FORMAT(ts, pattern): formats a UINT64 unix timestamp in milliseconds (UTC) as VARSIZED text.
/// The pattern follows Java's SimpleDateFormat for the numeric fields y, M, d, H, m, s and S;
/// text in single quotes is copied verbatim and every other character is copied as is.
/// The result is NULL iff any input is NULL.
class DateFormatLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "DATE_FORMAT";

    explicit DateFormatLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const DateFormatLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] DateFormatLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static LogicalFunctionRegistryReturnType createDATE_FORMAT(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<DateFormatLogicalFunction>;
};

namespace detail
{
struct ReflectedDateFormatLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<DateFormatLogicalFunction>
{
    DateFormatLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<DateFormatLogicalFunction>
{
    Reflected operator()(const DateFormatLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<DateFormatLogicalFunction>);
}

FMT_OSTREAM(NES::DateFormatLogicalFunction);
