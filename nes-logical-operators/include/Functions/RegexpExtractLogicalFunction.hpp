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

/// SQL REGEXP_EXTRACT(str, regex [, groupIndex]) with Flink semantics: returns capture group `groupIndex`
/// (default 0, the whole match) of the first match of `regex` (ECMAScript syntax) in `str`.
/// The result is always nullable: it is NULL if any input is NULL, if there is no match, if the group index is
/// out of range or the group did not participate in the match, or if a non-constant regex is invalid.
/// A constant regex is validated during type inference.
class RegexpExtractLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "REGEXP_EXTRACT";

    explicit RegexpExtractLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const RegexpExtractLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] RegexpExtractLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static LogicalFunctionRegistryReturnType createREGEXP_EXTRACT(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<RegexpExtractLogicalFunction>;
};

namespace detail
{
struct ReflectedRegexpExtractLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<RegexpExtractLogicalFunction>
{
    RegexpExtractLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<RegexpExtractLogicalFunction>
{
    Reflected operator()(const RegexpExtractLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<RegexpExtractLogicalFunction>);
}

FMT_OSTREAM(NES::RegexpExtractLogicalFunction);
