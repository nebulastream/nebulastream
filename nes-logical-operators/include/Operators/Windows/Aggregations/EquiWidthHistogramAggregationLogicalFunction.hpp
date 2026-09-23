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
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// Counts how many values of one unsigned integer field fall into each of numberOfBins equally wide bins over
/// minValue..maxValue, per window. Written as EQUIWIDTHHISTOGRAM(statisticId, field, budgetBytes, min, max).
///
/// The SQL call takes a memory budget, not a bin count, because a budget is what a user can reason about; create()
/// converts it once through equiWidthHistogramBinsForBudget and the budget does not outlive it. From there on the
/// histogram is defined by numberOfBins, minValue and maxValue alone -- that is what is stored, explained, hashed
/// and compared, and what the physical function and the blob agree on.
///
/// maxValue is inclusive and the bin width is an integer division, so the last bin is wider than the others by the
/// remainder plus maxValue itself; see EquiWidthHistogramBlobBinReader. A value outside minValue..maxValue is
/// counted in the last bin rather than dropped.
class EquiWidthHistogramAggregationLogicalFunction
{
public:
    EquiWidthHistogramAggregationLogicalFunction(
        AggregationFieldAccess inputFunction, uint64_t numberOfBins, uint64_t minValue, uint64_t maxValue);

    [[nodiscard]] EquiWidthHistogramAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] static DataType getAggregateType();
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] uint64_t getNumberOfBins() const;
    [[nodiscard]] uint64_t getMinValue() const;
    [[nodiscard]] uint64_t getMaxValue() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const EquiWidthHistogramAggregationLogicalFunction& other) const;

    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments);

    static constexpr std::string_view NAME = "EquiWidthHistogram";
    static constexpr bool IS_STATISTIC = true;

private:
    AggregationFieldAccess inputFunction;
    uint64_t numberOfBins;
    uint64_t minValue;
    uint64_t maxValue;
};

template <>
struct Reflector<EquiWidthHistogramAggregationLogicalFunction>
{
    Reflected operator()(const EquiWidthHistogramAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<EquiWidthHistogramAggregationLogicalFunction>
{
    EquiWidthHistogramAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::EquiWidthHistogramAggregationLogicalFunction>
{
    size_t operator()(const NES::EquiWidthHistogramAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::EquiWidthHistogramAggregationLogicalFunction>);
