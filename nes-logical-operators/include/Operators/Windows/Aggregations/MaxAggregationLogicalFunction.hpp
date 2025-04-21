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
#include <Abstract/LogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{

class MaxAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    static std::unique_ptr<WindowAggregationLogicalFunction> create(std::unique_ptr<LogicalFunction> onField);
    static std::unique_ptr<WindowAggregationLogicalFunction>
    create(std::unique_ptr<FieldAccessLogicalFunction> onField, std::unique_ptr<FieldAccessLogicalFunction> asField);
    MaxAggregationLogicalFunction(std::unique_ptr<LogicalFunction> onField, std::unique_ptr<LogicalFunction> asField);
    explicit MaxAggregationLogicalFunction(std::unique_ptr<FieldAccessLogicalFunction> onField);

    void inferStamp(const Schema& schema) override;
    std::unique_ptr<WindowAggregationLogicalFunction> clone() override;
    virtual ~MaxAggregationLogicalFunction() = default;
    NES::SerializableAggregationFunction serialize() const override;

private:
    static constexpr std::string_view NAME = "Max";
};
}
