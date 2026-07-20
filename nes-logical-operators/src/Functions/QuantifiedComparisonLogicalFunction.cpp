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

#include <Functions/QuantifiedComparisonLogicalFunction.hpp>

#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataTypeProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

QuantifiedComparisonLogicalFunction::QuantifiedComparisonLogicalFunction(
    std::vector<LogicalFunction> probeValues,
    LogicalOperator subqueryRoot,
    const Comparison comparison,
    const Quantifier quantifier)
    : probeValues(std::move(probeValues))
    , subqueryRoot(std::move(subqueryRoot))
    , comparison(comparison)
    , quantifier(quantifier)
    , dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN, DataType::NULLABLE::IS_NULLABLE))
{
    PRECONDITION(!this->probeValues.empty(), "A quantified comparison requires at least one probe value");
}

const std::vector<LogicalFunction>& QuantifiedComparisonLogicalFunction::getProbeValues() const
{
    return probeValues;
}

LogicalOperator QuantifiedComparisonLogicalFunction::getSubqueryRoot() const
{
    return subqueryRoot;
}

QuantifiedComparisonLogicalFunction::Comparison QuantifiedComparisonLogicalFunction::getComparison() const
{
    return comparison;
}

QuantifiedComparisonLogicalFunction::Quantifier QuantifiedComparisonLogicalFunction::getQuantifier() const
{
    return quantifier;
}

bool QuantifiedComparisonLogicalFunction::operator==(const QuantifiedComparisonLogicalFunction& rhs) const
{
    return probeValues == rhs.probeValues && subqueryRoot == rhs.subqueryRoot && comparison == rhs.comparison
        && quantifier == rhs.quantifier;
}

DataType QuantifiedComparisonLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction
QuantifiedComparisonLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.probeValues = probeValues
        | std::views::transform([&schema](const LogicalFunction& value) { return value.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    return copy;
}

std::vector<LogicalFunction> QuantifiedComparisonLogicalFunction::getChildren() const
{
    return probeValues;
}

QuantifiedComparisonLogicalFunction
QuantifiedComparisonLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == probeValues.size(), "The quantified comparison arity must not change");
    return QuantifiedComparisonLogicalFunction{children, subqueryRoot, comparison, quantifier};
}

std::string_view QuantifiedComparisonLogicalFunction::getType()
{
    return NAME;
}

std::string QuantifiedComparisonLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    const auto values
        = probeValues | std::views::transform([verbosity](const auto& value) { return value.explain(verbosity); });
    return fmt::format("({}) = ANY(SUBQUERY {})", fmt::join(values, ", "), subqueryRoot.getId());
}

Reflected Reflector<QuantifiedComparisonLogicalFunction>::operator()(const QuantifiedComparisonLogicalFunction&, const ReflectionContext&) const
{
    PRECONDITION(false, "Quantified comparisons must be extracted before reflection");
    std::unreachable();
}

QuantifiedComparisonLogicalFunction
Unreflector<QuantifiedComparisonLogicalFunction>::operator()(const Reflected&, const ReflectionContext&) const
{
    throw CannotDeserialize("QuantifiedComparisonLogicalFunction cannot be deserialized");
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterQuantifiedComparisonLogicalFunction(LogicalFunctionRegistryArguments)
{
    PRECONDITION(false, "QuantifiedComparisonLogicalFunction is created directly by the SQL parser");
    std::unreachable();
}

}
