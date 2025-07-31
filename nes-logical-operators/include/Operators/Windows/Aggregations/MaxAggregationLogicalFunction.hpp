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
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Schema.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class MaxAggregationLogicalFunction
{
public:
    explicit MaxAggregationLogicalFunction(AggregationFieldAccess inputFunction);
    MaxAggregationLogicalFunction(AggregationFieldAccess inputFunction, DataType aggregateType);

    [[nodiscard]] MaxAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] bool operator==(const MaxAggregationLogicalFunction& other) const;

private:
    AggregationFieldAccess inputFunction;
    DataType aggregateType;
    static constexpr std::string_view NAME = "Max";
};

template <>
struct Reflector<MaxAggregationLogicalFunction>
{
    Reflected operator()(const MaxAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<MaxAggregationLogicalFunction>
{
    MaxAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::MaxAggregationLogicalFunction>
{
    size_t operator()(const NES::MaxAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::MaxAggregationLogicalFunction>);
