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

#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class SumAggregationLogicalFunction
{
public:
    explicit SumAggregationLogicalFunction(AggregationFieldAccess inputFunction);
    SumAggregationLogicalFunction(AggregationFieldAccess inputFunction, DataType aggregateType);

    [[nodiscard]] SumAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const SumAggregationLogicalFunction& other) const;

private:
    [[nodiscard]] static DataType inferFromInput(DataType inputType);

    AggregationFieldAccess inputFunction;
    DataType aggregateType;
    static constexpr std::string_view NAME = "Sum";
};

template <>
struct Reflector<SumAggregationLogicalFunction>
{
    Reflected operator()(const SumAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<SumAggregationLogicalFunction>
{
    SumAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::SumAggregationLogicalFunction>
{
    size_t operator()(const NES::SumAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::SumAggregationLogicalFunction>);