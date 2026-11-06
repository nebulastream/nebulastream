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

/// SQL SPLIT_INDEX(str, delimiter, index) with Flink semantics: splits `str` at every occurrence of the
/// `delimiter` string (keeping empty parts) and returns the part at the 0-based `index`.
/// The result is always nullable: it is NULL if any input is NULL, if `str` is empty or if `index` is out of range.
class SplitIndexLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "SPLIT_INDEX";

    explicit SplitIndexLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const SplitIndexLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] SplitIndexLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static LogicalFunctionRegistryReturnType createSPLIT_INDEX(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<SplitIndexLogicalFunction>;
};

namespace detail
{
struct ReflectedSplitIndexLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

template <>
struct Unreflector<SplitIndexLogicalFunction>
{
    SplitIndexLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<SplitIndexLogicalFunction>
{
    Reflected operator()(const SplitIndexLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<SplitIndexLogicalFunction>);
}

FMT_OSTREAM(NES::SplitIndexLogicalFunction);
