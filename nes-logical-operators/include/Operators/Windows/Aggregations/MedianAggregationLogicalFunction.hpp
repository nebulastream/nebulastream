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
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class MedianAggregationLogicalFunction
{
public:
    /// Creates a new MedianAggregationLogicalFunction
    /// @param onField field on which the aggregation should be performed
    /// @param asField function describing how the aggregated field should be called
    MedianAggregationLogicalFunction(const FieldAccessLogicalFunction& onField, FieldAccessLogicalFunction asField);
    explicit MedianAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);
    ~MedianAggregationLogicalFunction() = default;

    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] MedianAggregationLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] MedianAggregationLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] MedianAggregationLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] MedianAggregationLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] MedianAggregationLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] MedianAggregationLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] bool operator==(const MedianAggregationLogicalFunction& otherMedianAggregationLogicalFunction) const;


private:
    static constexpr std::string_view NAME = "Median";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::FLOAT64;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::FLOAT64;

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<MedianAggregationLogicalFunction>);

template <>
struct Reflector<MedianAggregationLogicalFunction>
{
    Reflected operator()(const MedianAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<MedianAggregationLogicalFunction>
{
    MedianAggregationLogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedMedianAggregationLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};
}
