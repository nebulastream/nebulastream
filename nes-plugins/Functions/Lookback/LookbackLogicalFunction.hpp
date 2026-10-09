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

/// Threshold-triggered database enrichment.
///
/// `LOOKBACK(connection, sql, rowTemplate, param...)` runs a pre-defined query against a named
/// database connection and renders its rows through `rowTemplate`, returning one VARSIZED string.
/// No rows yields the empty string. The first three arguments must be constant VARSIZED literals;
/// the rest are bound to the SQL's `?` markers, in order, and are never interpolated into the
/// statement text.
///
/// The point of the function form is *when* it runs. A function in the SELECT list is evaluated by
/// a projection that sits above the selection — SQL puts WHERE before the SELECT list, and
/// PredicatePushdownRule pushes the predicate further down still — so the database is touched only
/// for tuples that breach the query's threshold. NebulaStream has no expression-level
/// short-circuiting to fall back on (AND/OR evaluate both operands, and there is no CASE), so
/// riding the operator boundary is what makes the "only on trigger" contract hold.
class LookbackLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "LOOKBACK";

    /// Number of leading arguments that must be constant VARSIZED literals: connection, SQL,
    /// row template.
    static constexpr size_t CONFIGURATION_ARGUMENTS = 3;

    explicit LookbackLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const LookbackLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LookbackLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] LookbackLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    static LogicalFunctionRegistryReturnType createLOOKBACK(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::vector<LogicalFunction> children;

    friend Reflector<LookbackLogicalFunction>;
};

template <>
struct Reflector<LookbackLogicalFunction>
{
    Reflected operator()(const LookbackLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<LookbackLogicalFunction>
{
    LookbackLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<LookbackLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedLookbackLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

FMT_OSTREAM(NES::LookbackLogicalFunction);
