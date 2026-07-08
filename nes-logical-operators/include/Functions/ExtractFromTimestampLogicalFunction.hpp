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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Selects which calendar component an ExtractFromTimestampLogicalFunction returns.
enum class TimestampUnit : uint8_t
{
    Day,
    Month,
    Year
};

/// Extracts a calendar component (day-of-month, month, or year) from a unix timestamp in
/// milliseconds (UINT64) and returns it as UINT64. The component is selected via `unit`.
class ExtractFromTimestampLogicalFunction final
{
public:
    ExtractFromTimestampLogicalFunction(TimestampUnit unit, LogicalFunction child);

    [[nodiscard]] bool operator==(const ExtractFromTimestampLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] ExtractFromTimestampLogicalFunction withDataType(const DataType& dataType) const;

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] ExtractFromTimestampLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] TimestampUnit getUnit() const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    TimestampUnit unit;
    DataType outputType;
    LogicalFunction child;

    friend Reflector<ExtractFromTimestampLogicalFunction>;
};

template <>
struct Reflector<ExtractFromTimestampLogicalFunction>
{
    Reflected operator()(const ExtractFromTimestampLogicalFunction& function) const;
};

template <>
struct Unreflector<ExtractFromTimestampLogicalFunction>
{
    ExtractFromTimestampLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<ExtractFromTimestampLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedExtractFromTimestampLogicalFunction
{
    TimestampUnit unit;
    LogicalFunction child;
};
}

FMT_OSTREAM(NES::ExtractFromTimestampLogicalFunction);
