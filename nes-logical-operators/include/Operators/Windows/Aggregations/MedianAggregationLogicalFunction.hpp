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
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class MedianAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    /// Creates a new MedianAggregationLogicalFunction
    /// @param onField field on which the aggregation should be performed
    /// @param asField function describing how the aggregated field should be called
    MedianAggregationLogicalFunction(const FieldAccessLogicalFunction& onField, FieldAccessLogicalFunction asField);
    explicit MedianAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);

    void inferStamp(const Schema& schema) override;

    ~MedianAggregationLogicalFunction() override = default;

    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] Reflected reflect() const override;


private:
    static constexpr std::string_view NAME = "Median";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::FLOAT64;
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

namespace NES::detail
{
struct ReflectedMedianAggregationLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};
}
