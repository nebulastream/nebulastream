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
#include <optional>
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

class CountAggregationLogicalFunction
{
public:
    CountAggregationLogicalFunction(FieldAccessLogicalFunction onField, FieldAccessLogicalFunction asField);
    explicit CountAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);
    ~CountAggregationLogicalFunction() = default;

    [[nodiscard]] std::string_view getName() const noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] CountAggregationLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] CountAggregationLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] CountAggregationLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] CountAggregationLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] CountAggregationLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] CountAggregationLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] bool operator==(const CountAggregationLogicalFunction& otherCountAggregationLogicalFunction) const;


private:
    static constexpr std::string_view NAME = "Count";
    static constexpr DataType::Type inputAggregateStampType = DataType::Type::UINT64;
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::FLOAT64;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::FLOAT64;

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<CountAggregationLogicalFunction>);

template <>
struct Reflector<CountAggregationLogicalFunction>
{
    Reflected operator()(const CountAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<CountAggregationLogicalFunction>
{
    CountAggregationLogicalFunction operator()(const Reflected& reflected) const;
};

}

namespace NES::detail
{
struct ReflectedCountAggregationLogicalFunction
{
    std::optional<FieldAccessLogicalFunction> onField;
    std::optional<FieldAccessLogicalFunction> asField;
};

}
