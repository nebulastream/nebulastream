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
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Reflection.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class MaxAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& onField, FieldAccessLogicalFunction asField);
    explicit MaxAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);

    void inferStamp(const Schema& schema) override;
    ~MaxAggregationLogicalFunction() override = default;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool shallIncludeNullValues() const noexcept override;
    [[nodiscard]] Reflected reflect() const override;

private:
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
