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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// fmt::format-style string formatter.
///   Format(VARSIZED("Value is: {}, larger than: {}"), value, otherValue)
/// First child is the format string, subsequent children fill the `{}` placeholders
/// left-to-right. Output is VARSIZED.
class FormatLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Format";

    explicit FormatLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const FormatLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] FormatLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] FormatLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<FormatLogicalFunction>;
};

template <>
struct Reflector<FormatLogicalFunction>
{
    Reflected operator()(const FormatLogicalFunction& function) const;
};

template <>
struct Unreflector<FormatLogicalFunction>
{
    FormatLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<FormatLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedFormatLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

FMT_OSTREAM(NES::FormatLogicalFunction);
