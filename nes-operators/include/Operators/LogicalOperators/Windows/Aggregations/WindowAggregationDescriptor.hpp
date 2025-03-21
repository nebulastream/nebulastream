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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES::Windowing
{
/**
 * Abstract class for window aggregations. All window aggregations operate on a field and output another field.
 */
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
        Median,
        None
    };

    /**
    * Defines the field to which a aggregate output is assigned.
    * @param asField
    * @return WindowAggregationDescriptor
    */
    std::shared_ptr<WindowAggregationDescriptor> as(const std::shared_ptr<NodeFunction>& asField);

    /**
    * Returns the result field of the aggregation
    * @return std::shared_ptr<NodeFunction>
    */
    [[nodiscard]] std::shared_ptr<NodeFunction> as() const;

    /**
    * Returns the result field of the aggregation
    * @return std::shared_ptr<NodeFunction>
    */
    [[nodiscard]] std::shared_ptr<NodeFunction> on() const;

    /**
     * @brief Returns the type of this aggregation.
     * @return WindowAggregationDescriptor::Type
     */
    Type getType() const;

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    virtual void inferStamp(const Schema& schema) = 0;

    /**
    * @brief Creates a deep copy of the window aggregation
    */
    virtual std::shared_ptr<WindowAggregationDescriptor> copy() = 0;

    /**
     * @return the input type
     */
    virtual std::shared_ptr<DataType> getInputStamp() = 0;

    /**
     * @return the final aggregation type
     */
    virtual std::shared_ptr<DataType> getFinalAggregateStamp() = 0;

    std::string toString() const;

    std::string getTypeAsString() const;

    /**
     * @brief Check if input window aggregation is equal to this window aggregation definition by checking the aggregation type,
     * on field, and as field
     * @param otherWindowAggregationDescriptor : other window aggregation definition
     * @return true if equal else false
     */
    [[nodiscard]] bool equal(const std::shared_ptr<WindowAggregationDescriptor>& otherWindowAggregationDescriptor) const;

protected:
    explicit WindowAggregationDescriptor(const std::shared_ptr<NodeFunctionFieldAccess>& onField);
    WindowAggregationDescriptor(const std::shared_ptr<NodeFunction>& onField, const std::shared_ptr<NodeFunction>& asField);
    WindowAggregationDescriptor() = default;
    std::shared_ptr<NodeFunction> onField;
    std::shared_ptr<NodeFunction> asField;
    Type aggregationType;
};
}
