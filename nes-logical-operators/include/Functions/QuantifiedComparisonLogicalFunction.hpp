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
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// A typed SQL quantified comparison that still owns its relational subquery.
/// A later semantic phase extracts it into a MarkApplyLogicalOperator. Keeping
/// this node type-correct lets it occur in every expression-bearing operator
/// without teaching those operators about subqueries.
class QuantifiedComparisonLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "QuantifiedComparison";

    enum class Comparison : uint8_t
    {
        EQUALS
    };

    enum class Quantifier : uint8_t
    {
        ANY
    };

    QuantifiedComparisonLogicalFunction(
        std::vector<LogicalFunction> probeValues,
        LogicalOperator subqueryRoot,
        Comparison comparison = Comparison::EQUALS,
        Quantifier quantifier = Quantifier::ANY);

    [[nodiscard]] const std::vector<LogicalFunction>& getProbeValues() const;
    [[nodiscard]] LogicalOperator getSubqueryRoot() const;
    [[nodiscard]] Comparison getComparison() const;
    [[nodiscard]] Quantifier getQuantifier() const;

    [[nodiscard]] bool operator==(const QuantifiedComparisonLogicalFunction& rhs) const;
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] QuantifiedComparisonLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    [[nodiscard]] static std::string_view getType();
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    std::vector<LogicalFunction> probeValues;
    LogicalOperator subqueryRoot;
    Comparison comparison;
    Quantifier quantifier;
    DataType dataType;
};

template <>
struct Reflector<QuantifiedComparisonLogicalFunction>
{
    Reflected operator()(const QuantifiedComparisonLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<QuantifiedComparisonLogicalFunction>
{
    QuantifiedComparisonLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<QuantifiedComparisonLogicalFunction>);

}
