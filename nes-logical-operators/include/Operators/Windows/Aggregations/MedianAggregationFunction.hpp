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
#include <Operators/Windows/Aggregations/WindowAggregationFunction.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

class MedianAggregationFunction : public WindowAggregationFunction
{
public:
    static constexpr std::string_view NAME = "Median";

    static std::shared_ptr<WindowAggregationFunction> on(const std::shared_ptr<LogicalFunction>& onField);

    /// Creates a new MedianAggregationFunction
    /// @param onField field on which the aggregation should be performed
    /// @param asField function describing how the aggregated field should be called
    static std::shared_ptr<WindowAggregationFunction>
    create(std::shared_ptr<FieldAccessLogicalFunction> onField, std::shared_ptr<FieldAccessLogicalFunction> asField);

    void inferStamp(const Schema& schema) override;

    std::shared_ptr<WindowAggregationFunction> clone() override;

    std::shared_ptr<DataType> getInputStamp() override;
    std::shared_ptr<DataType> getPartialAggregateStamp() override;
    std::shared_ptr<DataType> getFinalAggregateStamp() override;

    virtual ~MedianAggregationFunction() = default;

    NES::SerializableAggregationFunction serialize() const override;

private:
    explicit MedianAggregationFunction(std::shared_ptr<FieldAccessLogicalFunction> onField);
    MedianAggregationFunction(std::shared_ptr<LogicalFunction> onField, std::shared_ptr<LogicalFunction> asField);
};
}
