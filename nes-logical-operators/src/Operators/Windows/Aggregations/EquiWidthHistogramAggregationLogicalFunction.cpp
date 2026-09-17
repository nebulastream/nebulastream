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

#include <Operators/Windows/Aggregations/EquiWidthHistogramAggregationLogicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Statistic/EquiWidthHistogramBlobLayout.hpp>
#include <Operators/Windows/Aggregations/AggregationParameters.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

EquiWidthHistogramAggregationLogicalFunction::EquiWidthHistogramAggregationLogicalFunction(
    AggregationFieldAccess inputFunction, const uint64_t numberOfBins, const uint64_t minValue, const uint64_t maxValue)
    : inputFunction(std::move(inputFunction)), numberOfBins(numberOfBins), minValue(minValue), maxValue(maxValue)
{
}

std::string_view EquiWidthHistogramAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

DataType EquiWidthHistogramAggregationLogicalFunction::getAggregateType()
{
    return DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
}

bool EquiWidthHistogramAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    /// Moot: a nullable input never gets past withInferredType, so there is no null to include or skip.
    return false;
}

AggregationFieldAccess EquiWidthHistogramAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

uint64_t EquiWidthHistogramAggregationLogicalFunction::getNumberOfBins() const
{
    return numberOfBins;
}

uint64_t EquiWidthHistogramAggregationLogicalFunction::getMinValue() const
{
    return minValue;
}

uint64_t EquiWidthHistogramAggregationLogicalFunction::getMaxValue() const
{
    return maxValue;
}

std::string EquiWidthHistogramAggregationLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}({})", NAME, numberOfBins);
    }
    const auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({}, bins: {}, range: {}..{})", NAME, inputExplain, numberOfBins, minValue, maxValue);
}

bool EquiWidthHistogramAggregationLogicalFunction::operator==(const EquiWidthHistogramAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction and numberOfBins == other.numberOfBins and minValue == other.minValue
        and maxValue == other.maxValue;
}

EquiWidthHistogramAggregationLogicalFunction
EquiWidthHistogramAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);
    const auto inputType = newInputFunction->getDataType();

    /// The bins are laid out over unsigned integers, and lift() bins the raw value without a conversion. A signed or
    /// fractional input would have to be converted to get here, and a converted value is not the value the user
    /// asked about, so it is rejected instead of silently reinterpreted.
    if (not inputType.isInteger() or inputType.isSignedInteger())
    {
        throw CannotInferSchema(
            "{} counts unsigned integers, but {} is {}. Cast the field to an unsigned integer type first.",
            NAME,
            newInputFunction->getField().getFullyQualifiedName(),
            inputType);
    }
    if (inputType.nullable)
    {
        throw CannotInferSchema(
            "{} has no bin for a null value, but {} is nullable. Filter the nulls out first.",
            NAME,
            newInputFunction->getField().getFullyQualifiedName());
    }
    return EquiWidthHistogramAggregationLogicalFunction{newInputFunction, numberOfBins, minValue, maxValue};
}

namespace detail
{
struct ReflectedEquiWidthHistogramAggregationLogicalFunction
{
    AggregationFieldAccess inputFunction;
    uint64_t numberOfBins;
    uint64_t minValue;
    uint64_t maxValue;
};
}

Reflected Reflector<EquiWidthHistogramAggregationLogicalFunction>::operator()(
    const EquiWidthHistogramAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedEquiWidthHistogramAggregationLogicalFunction{
        .inputFunction = function.getInputFunction(),
        .numberOfBins = function.getNumberOfBins(),
        .minValue = function.getMinValue(),
        .maxValue = function.getMaxValue()});
}

EquiWidthHistogramAggregationLogicalFunction
Unreflector<EquiWidthHistogramAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [inputFunction, numberOfBins, minValue, maxValue]
        = context.unreflect<detail::ReflectedEquiWidthHistogramAggregationLogicalFunction>(reflected);
    return EquiWidthHistogramAggregationLogicalFunction{std::move(inputFunction), numberOfBins, minValue, maxValue};
}

AggregationLogicalFunctionRegistryReturnType
EquiWidthHistogramAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    /// The parser has already taken the statisticId off the front of EQUIWIDTHHISTOGRAM(id, field, budget, min, max).
    static constexpr auto CALL = "EQUIWIDTHHISTOGRAM(statisticId, field, budgetInBytes, min, max)";
    if (arguments.parameters.size() != 4)
    {
        throw InvalidQuerySyntax("{} takes four arguments after the statisticId, but got {}", CALL, arguments.parameters.size());
    }
    auto inputFunction = parseFieldParameter(arguments.parameters[0], "the field of EQUIWIDTHHISTOGRAM");
    const auto budgetInBytes = parseUnsignedParameter(arguments.parameters[1], "the memory budget of EQUIWIDTHHISTOGRAM");
    const auto minValue = parseUnsignedParameter(arguments.parameters[2], "the minimum of EQUIWIDTHHISTOGRAM");
    const auto maxValue = parseUnsignedParameter(arguments.parameters[3], "the maximum of EQUIWIDTHHISTOGRAM");

    if (minValue >= maxValue)
    {
        throw InvalidQuerySyntax("{} covers the values min..max, so max must be above min, but got {}..{}", CALL, minValue, maxValue);
    }

    /// The budget is the only thing the caller states about the size; the bin count follows from it and is what the
    /// histogram is built from. Both errors below therefore name the budget that would have worked, because the
    /// caller never sees a bin count.
    const auto numberOfBins = equiWidthHistogramBinsForBudget(budgetInBytes);
    if (numberOfBins == 0)
    {
        throw InvalidQuerySyntax(
            "{} needs a budget of at least {} bytes to pay for a single bin, but got {}",
            CALL,
            equiWidthHistogramBudgetForBins(1),
            budgetInBytes);
    }
    if (maxValue - minValue < numberOfBins)
    {
        /// Bins narrower than one value would make the bin width zero and divide by it. Rejected rather than
        /// clamped: a caller that asked for more resolution than the range has should hear about it.
        throw InvalidQuerySyntax(
            "{} with a budget of {} bytes asks for {} bins, but {}..{} only holds {} values. The largest budget this "
            "range accepts is {} bytes.",
            CALL,
            budgetInBytes,
            numberOfBins,
            minValue,
            maxValue,
            maxValue - minValue,
            equiWidthHistogramBudgetForBins(maxValue - minValue));
    }
    return EquiWidthHistogramAggregationLogicalFunction{std::move(inputFunction), numberOfBins, minValue, maxValue};
}

}

size_t std::hash<NES::EquiWidthHistogramAggregationLogicalFunction>::operator()(
    const NES::EquiWidthHistogramAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(
        aggregationFunction.getInputFunction(),
        aggregationFunction.getName(),
        aggregationFunction.getNumberOfBins(),
        aggregationFunction.getMinValue(),
        aggregationFunction.getMaxValue());
}
