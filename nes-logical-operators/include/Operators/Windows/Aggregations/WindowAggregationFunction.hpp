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

class WindowAggregationFunction
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
    virtual ~WindowAggregationFunction() = default;

    /// Defines the field to which a aggregate output is assigned.
    std::unique_ptr<WindowAggregationFunction> as(std::unique_ptr<LogicalFunction> asField);

    /// Returns the result field of the aggregation
    LogicalFunction& as() const;

    //// Returns the result field of the aggregation
    LogicalFunction& on() const;

    /// Returns the type of this aggregation.
    Type getType() const;

    /// @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(const Schema& schema) = 0;

    /// @brief Creates a deep copy of the window aggregation
    virtual std::unique_ptr<WindowAggregationFunction> clone() = 0;

    DataType& getInputStamp() const;
    DataType& getPartialAggregateStamp() const;
    DataType& getFinalAggregateStamp() const;

    std::string toString() const;

    std::string getTypeAsString() const;

    bool operator==(std::shared_ptr<WindowAggregationFunction> otherWindowAggregationFunction) const;

    virtual NES::SerializableAggregationFunction serialize() const = 0;

protected:
    explicit WindowAggregationFunction(
        std::unique_ptr<DataType> inputStamp, std::unique_ptr<DataType> partialAggregateStamp, std::unique_ptr<DataType> finalAggregateStamp, std::unique_ptr<LogicalFunction> onField, std::unique_ptr<LogicalFunction> asField);
    explicit WindowAggregationFunction(std::unique_ptr<DataType> inputStamp, std::unique_ptr<DataType> partialAggregateStamp,
                                       std::unique_ptr<DataType> finalAggregateStamp, std::unique_ptr<FieldAccessLogicalFunction> onField);
    WindowAggregationFunction() = default;
    std::unique_ptr<DataType> inputStamp;
    std::unique_ptr<DataType> partialAggregateStamp;
    std::unique_ptr<DataType> finalAggregateStamp;
    std::unique_ptr<LogicalFunction> onField;
    std::unique_ptr<LogicalFunction> asField;
    Type aggregationType;
};
}
