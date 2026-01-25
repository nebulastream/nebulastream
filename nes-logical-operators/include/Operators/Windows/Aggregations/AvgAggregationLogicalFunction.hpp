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
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class AvgAggregationLogicalFunction final
{
public:
    AvgAggregationLogicalFunction(const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField);
    explicit AvgAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);
    ~AvgAggregationLogicalFunction() = default;
    
    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] SerializableAggregationFunction serialize() const;
    [[nodiscard]] bool equals(const AvgAggregationLogicalFunction& other) const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    void inferStamp(const Schema& schema);
    void setInputStamp(DataType inputStamp);
    void setPartialAggregateStamp(DataType partialAggregateStamp);
    void setFinalAggregateStamp(DataType finalAggregateStamp);
    void setOnField(FieldAccessLogicalFunction onField);
    void setAsField(FieldAccessLogicalFunction asField);

    [[nodiscard]] bool operator==(const AvgAggregationLogicalFunction& rhs) const;

protected:
    explicit AvgAggregationLogicalFunction(
        DataType inputStamp,
        DataType partialAggregateStamp,
        DataType finalAggregateStamp,
        FieldAccessLogicalFunction onField,
        FieldAccessLogicalFunction asField);

    explicit AvgAggregationLogicalFunction(
        DataType inputStamp, DataType partialAggregateStamp, DataType finalAggregateStamp, const FieldAccessLogicalFunction& onField);

private:
    static constexpr std::string_view NAME = "Avg";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::FLOAT64;

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<AvgAggregationLogicalFunction>);

}
