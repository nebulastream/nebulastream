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
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class ReservoirSampleAggregationLogicalFunction
{
public:
    ReservoirSampleAggregationLogicalFunction(uint64_t sampleSize, uint64_t seed);
    ReservoirSampleAggregationLogicalFunction(AggregationFieldAccess inputFunction, uint64_t sampleSize, uint64_t seed);

    [[nodiscard]] ReservoirSampleAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] static DataType getAggregateType();
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] uint64_t getSampleSize() const;
    [[nodiscard]] uint64_t getSeed() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const ReservoirSampleAggregationLogicalFunction& other) const;

private:
    AggregationFieldAccess inputFunction;
    uint64_t sampleSize;
    uint64_t seed;
    static constexpr std::string_view NAME = "ReservoirSample";
    static constexpr std::string_view PlaceholderFieldName = "RESERVOIRSAMPLEINPUT";
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;
};

template <>
struct Reflector<ReservoirSampleAggregationLogicalFunction>
{
    Reflected operator()(const ReservoirSampleAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ReservoirSampleAggregationLogicalFunction>
{
    ReservoirSampleAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::ReservoirSampleAggregationLogicalFunction>
{
    size_t operator()(const NES::ReservoirSampleAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::ReservoirSampleAggregationLogicalFunction>);
