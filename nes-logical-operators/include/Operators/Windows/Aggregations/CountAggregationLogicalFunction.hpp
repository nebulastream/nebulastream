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
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class CountAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    CountAggregationLogicalFunction(FieldAccessLogicalFunction onField, FieldAccessLogicalFunction asField);
    explicit CountAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);
    ~CountAggregationLogicalFunction() override = default;

    void inferStamp(const Schema& schema) override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool shallIncludeNullValues() const noexcept override;
    [[nodiscard]] Reflected reflect() const override;

private:
    static constexpr std::string_view NAME = "Count";
    static constexpr DataType::Type inputAggregateStampType = DataType::Type::UINT64;
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::FLOAT64;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::FLOAT64;
};

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
