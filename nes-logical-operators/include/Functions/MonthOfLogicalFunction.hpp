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
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Extracts the month (1-12) from a unix timestamp in milliseconds (UINT64) and returns it as UINT64.
class MonthOfLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Month_Of";

    explicit MonthOfLogicalFunction(LogicalFunction child);

    [[nodiscard]] bool operator==(const MonthOfLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] MonthOfLogicalFunction withDataType(const DataType& dataType) const;

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] MonthOfLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] static std::string_view getType();
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType outputType;
    LogicalFunction child;

    friend Reflector<MonthOfLogicalFunction>;
};

template <>
struct Reflector<MonthOfLogicalFunction>
{
    Reflected operator()(const MonthOfLogicalFunction& function) const;
};

template <>
struct Unreflector<MonthOfLogicalFunction>
{
    MonthOfLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<MonthOfLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedMonthOfLogicalFunction
{
    LogicalFunction child;
};
}

FMT_OSTREAM(NES::MonthOfLogicalFunction);
