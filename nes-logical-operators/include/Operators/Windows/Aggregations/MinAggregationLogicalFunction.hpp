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
#include <API/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class MinAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction> create(FieldAccessLogicalFunction onField);
    static std::shared_ptr<WindowAggregationLogicalFunction> create(FieldAccessLogicalFunction onField, FieldAccessLogicalFunction asField);

    MinAggregationLogicalFunction(FieldAccessLogicalFunction onField, FieldAccessLogicalFunction asField);
    explicit MinAggregationLogicalFunction(FieldAccessLogicalFunction onField);
    virtual ~MinAggregationLogicalFunction() = default;

    void inferStamp(const Schema& schema) override;
    NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;

private:
    static constexpr std::string_view NAME = "Min";
};
}
