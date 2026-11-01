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

/// `UNNEST(arr)` in a projection: a placeholder for one element of the VARSIZED JSON array `arr`. It is never executed:
/// `UnnestTranslationRule` replaces it by a field that an `UnnestLogicalOperator` below the projection fills, one row per
/// element.
class UnnestLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "UNNEST";

    /// NOLINTNEXTLINE(modernize-pass-by-value)
    explicit UnnestLogicalFunction(const LogicalFunction& child);

    [[nodiscard]] bool operator==(const UnnestLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] UnnestLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] UnnestLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    [[nodiscard]] LogicalFunction getChild() const;

    /// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// NOLINTNEXTLINE(readability-identifier-naming)
    static LogicalFunctionRegistryReturnType createUNNEST(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    LogicalFunction child;

    friend Reflector<UnnestLogicalFunction>;
};

template <>
struct Reflector<UnnestLogicalFunction>
{
    Reflected operator()(const UnnestLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<UnnestLogicalFunction>
{
    UnnestLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<UnnestLogicalFunction>);
}

namespace NES::detail
{
struct ReflectedUnnestLogicalFunction
{
    std::optional<LogicalFunction> child;
};
}

FMT_OSTREAM(NES::UnnestLogicalFunction);
