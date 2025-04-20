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

namespace NES::Windowing
{

class WindowAggregationDescriptor
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

    /// Defines the field to which a aggregate output is assigned.
    std::shared_ptr<WindowAggregationDescriptor> as(const std::shared_ptr<LogicalFunction>& asField);

    /// Returns the result field of the aggregation
    std::shared_ptr<LogicalFunction> as() const;

    //// Returns the result field of the aggregation
    std::shared_ptr<LogicalFunction> on() const;

    /// Returns the type of this aggregation.
    Type getType() const;

    /// @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
    virtual void inferStamp(std::shared_ptr<Schema> schema) = 0;

    /// @brief Creates a deep copy of the window aggregation
    virtual std::shared_ptr<WindowAggregationDescriptor> clone() = 0;

    virtual std::shared_ptr<DataType> getInputStamp() = 0;
    virtual std::shared_ptr<DataType> getPartialAggregateStamp() = 0;
    virtual std::shared_ptr<DataType> getFinalAggregateStamp() = 0;

    std::string toString() const;

    std::string getTypeAsString() const;

    bool equal(std::shared_ptr<WindowAggregationDescriptor> otherWindowAggregationDescriptor) const;

protected:
    explicit WindowAggregationDescriptor(const std::shared_ptr<FieldAccessLogicalFunction>& onField);
    WindowAggregationDescriptor(const std::shared_ptr<LogicalFunction>& onField, const std::shared_ptr<LogicalFunction>& asField);
    WindowAggregationDescriptor() = default;
    std::shared_ptr<LogicalFunction> onField;
    std::shared_ptr<LogicalFunction> asField;
    Type aggregationType;
};
}
