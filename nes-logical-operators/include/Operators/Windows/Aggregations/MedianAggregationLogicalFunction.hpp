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

class MedianAggregationLogicalFunction
{
public:
    explicit MedianAggregationLogicalFunction(AggregationFieldAccess inputFunction);

    [[nodiscard]] MedianAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const MedianAggregationLogicalFunction& other) const;

private:
    AggregationFieldAccess inputFunction;
    static constexpr std::string_view NAME = "Median";
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::FLOAT64;
};

template <>
struct Reflector<MedianAggregationLogicalFunction>
{
    Reflected operator()(const MedianAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<MedianAggregationLogicalFunction>
{
    MedianAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::MedianAggregationLogicalFunction>
{
    size_t operator()(const NES::MedianAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::MedianAggregationLogicalFunction>);
