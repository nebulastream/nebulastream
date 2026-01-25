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

class MaxAggregationLogicalFunction
{
public:
    MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& onField, FieldAccessLogicalFunction asField);
    explicit MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);
    ~MaxAggregationLogicalFunction() = default;

    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] std::string toString() const;
    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;
    [[nodiscard]] FieldAccessLogicalFunction getOnField() const;
    [[nodiscard]] FieldAccessLogicalFunction getAsField() const;

    [[nodiscard]] Reflected reflect() const;
    [[nodiscard]] MaxAggregationLogicalFunction withInferredStamp(const Schema& schema) const;
    [[nodiscard]] MaxAggregationLogicalFunction withInputStamp(DataType inputStamp) const;
    [[nodiscard]] MaxAggregationLogicalFunction withPartialAggregateStamp(DataType partialAggregateStamp) const;
    [[nodiscard]] MaxAggregationLogicalFunction withFinalAggregateStamp(DataType finalAggregateStamp) const;
    [[nodiscard]] MaxAggregationLogicalFunction withOnField(FieldAccessLogicalFunction onField) const;
    [[nodiscard]] MaxAggregationLogicalFunction withAsField(FieldAccessLogicalFunction asField) const;

    [[nodiscard]] bool operator==(const MaxAggregationLogicalFunction& otherMaxAggregationLogicalFunction) const;


private:
    static constexpr std::string_view NAME = "Max";

    DataType inputStamp;
    DataType partialAggregateStamp;
    DataType finalAggregateStamp;
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

static_assert(WindowAggregationFunctionConcept<MaxAggregationLogicalFunction>);

template <>
struct Reflector<MaxAggregationLogicalFunction>
{
    Reflected operator()(const MaxAggregationLogicalFunction& function) const;
};

template <>
struct Unreflector<MaxAggregationLogicalFunction>
{
    MaxAggregationLogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedMaxAggregationLogicalFunction
{
    FieldAccessLogicalFunction onField;
    FieldAccessLogicalFunction asField;
};

}
