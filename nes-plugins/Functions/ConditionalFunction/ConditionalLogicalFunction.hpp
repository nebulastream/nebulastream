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

#include <optional>
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

/// @brief Represents a CASE WHEN expression. Children are stored as a flat list:
/// [cond1, result1, cond2, result2, ..., condN, resultN, default]
/// The list must have an odd number of elements >= 3.
class ConditionalLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Conditional";

    explicit ConditionalLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const ConditionalLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;

    [[nodiscard]] ConditionalLogicalFunction withDataType(const DataType& dataType) const;

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;

    [[nodiscard]] ConditionalLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<ConditionalLogicalFunction>;
};

template <>
struct Reflector<ConditionalLogicalFunction>
{
    Reflected operator()(const ConditionalLogicalFunction& function) const;
};

template <>
struct Unreflector<ConditionalLogicalFunction>
{
    ConditionalLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<ConditionalLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedConditionalLogicalFunction
{
    std::vector<std::optional<LogicalFunction>> children;
};
}

FMT_OSTREAM(NES::ConditionalLogicalFunction);
