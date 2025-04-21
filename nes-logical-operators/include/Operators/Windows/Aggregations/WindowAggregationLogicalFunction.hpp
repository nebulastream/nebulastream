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

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>

namespace NES
{

class WindowAggregationLogicalFunction
{
public:
    enum class Type : uint8_t
    {
        Avg,
        Count,
        Max,
        Min,
        Sum,
        Median
    };
    virtual ~WindowAggregationLogicalFunction() = default;

    /// Defines the field to which a aggregate output is assigned.
    std::unique_ptr<WindowAggregationLogicalFunction> as(const FieldAccessLogicalFunction& asField);

    /// Returns the result field of the aggregation
    FieldAccessLogicalFunction as() const;

    //// Returns the result field of the aggregation
    FieldAccessLogicalFunction on() const;

    /// Returns the type of this aggregation.
    Type getType() const;

    /// @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema) = 0;

    /// @brief Creates a deep copy of the window aggregation
    virtual std::unique_ptr<WindowAggregationLogicalFunction> clone() = 0;

    std::shared_ptr<DataType> getInputStamp() const;
    std::shared_ptr<DataType> getPartialAggregateStamp() const;
    std::shared_ptr<DataType> getFinalAggregateStamp() const;

    std::string toString() const;

    std::string getTypeAsString() const;

    bool operator==(std::shared_ptr<WindowAggregationLogicalFunction> otherWindowAggregationLogicalFunction) const;

    virtual NES::SerializableAggregationFunction serialize() const = 0;

protected:
    explicit WindowAggregationLogicalFunction(
        std::shared_ptr<DataType> inputStamp,
        std::shared_ptr<DataType> partialAggregateStamp,
        std::shared_ptr<DataType> finalAggregateStamp,
        FieldAccessLogicalFunction onField,
        FieldAccessLogicalFunction asField);
    explicit WindowAggregationLogicalFunction(
        std::shared_ptr<DataType> inputStamp,
        std::shared_ptr<DataType> partialAggregateStamp,
        std::shared_ptr<DataType> finalAggregateStamp,
        FieldAccessLogicalFunction onField);
    WindowAggregationLogicalFunction() = delete;

    std::shared_ptr<DataType> inputStamp, partialAggregateStamp, finalAggregateStamp;
    FieldAccessLogicalFunction onField, asField;
    Type aggregationType;
};
}
