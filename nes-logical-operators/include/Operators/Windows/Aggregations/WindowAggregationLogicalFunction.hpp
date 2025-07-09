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
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

class WindowAggregationLogicalFunction
{
public:
    virtual ~WindowAggregationLogicalFunction() = default;

    [[nodiscard]] DataType getInputStamp() const;
    [[nodiscard]] DataType getPartialAggregateStamp() const;
    [[nodiscard]] DataType getFinalAggregateStamp() const;

    [[nodiscard]] std::string toString() const;
    bool operator==(const std::shared_ptr<WindowAggregationLogicalFunction>& otherWindowAggregationLogicalFunction) const;

    /// @brief Infers the dataType of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema) = 0;

    [[nodiscard]] virtual SerializableAggregationFunction serialize() const = 0;

    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;

    ///@return If this is set, the aggregations lift function will be called with buffers in sequence number order.
    [[nodiscard]] virtual bool requiresSequentialAggregation() const;

    DataType inputStamp, partialAggregateStamp, finalAggregateStamp;
    FieldAccessLogicalFunction onField, asField;

protected:
    explicit WindowAggregationLogicalFunction(
        DataType inputStamp,
        DataType partialAggregateStamp,
        DataType finalAggregateStamp,
        FieldAccessLogicalFunction onField,
        FieldAccessLogicalFunction asField);

    explicit WindowAggregationLogicalFunction(
        DataType inputStamp, DataType partialAggregateStamp, DataType finalAggregateStamp, const FieldAccessLogicalFunction& onField);
};
}
