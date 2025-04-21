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

class MedianAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    MedianAggregationLogicalFunction(LogicalFunction onField, LogicalFunction asField);
    static std::unique_ptr<WindowAggregationLogicalFunction> create(LogicalFunction onField);
    explicit MedianAggregationLogicalFunction(const FieldAccessLogicalFunction& onField);

    /// Creates a new MedianAggregationLogicalFunction
    /// @param onField field on which the aggregation should be performed
    /// @param asField function describing how the aggregated field should be called
    static std::unique_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& onField, const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;

    std::unique_ptr<WindowAggregationLogicalFunction> clone() override;
    virtual ~MedianAggregationLogicalFunction() = default;

    NES::SerializableAggregationFunction serialize() const override;

private:
    static constexpr std::string_view NAME = "Median";
};
}
