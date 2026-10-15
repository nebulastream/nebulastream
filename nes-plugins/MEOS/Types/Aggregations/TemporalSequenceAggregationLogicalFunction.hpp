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
#include <DataTypes/DataType.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{

/// Creates a TemporalPointSequence value out of the incoming TemporalPoint values.
class TemporalSequenceAggregationLogicalFunction final
{
public:
    explicit TemporalSequenceAggregationLogicalFunction(AggregationFieldAccess inputFunction);

    [[nodiscard]] TemporalSequenceAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const TemporalSequenceAggregationLogicalFunction& other) const;

    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments);
private:
    AggregationFieldAccess inputFunction;
    DataType aggregateType;
    static constexpr std::string_view NAME = "TEMPORAL_SEQUENCE";
};

template <>
struct Reflector<TemporalSequenceAggregationLogicalFunction>
{
    Reflected operator()(const TemporalSequenceAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TemporalSequenceAggregationLogicalFunction>
{
    TemporalSequenceAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::TemporalSequenceAggregationLogicalFunction>
{
    size_t operator()(const NES::TemporalSequenceAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::TemporalSequenceAggregationLogicalFunction>);
