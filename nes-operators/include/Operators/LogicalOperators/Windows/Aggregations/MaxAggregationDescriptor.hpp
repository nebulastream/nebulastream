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

#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>
namespace NES::Windowing
{

/**
 * @brief
 * The MaxAggregationDescriptor aggregation calculates the maximum over the window.
 */
class MaxAggregationDescriptor : public WindowAggregationDescriptor
{
public:
    /**
     * Factory method to create a MaxAggregationDescriptor aggregation on a particular field.
     */
    static WindowAggregationDescriptorPtr on(const NodeFunctionPtr& onField);

    static WindowAggregationDescriptorPtr create(NodeFunctionFieldAccessPtr onField, NodeFunctionFieldAccessPtr asField);

    DataTypePtr getInputStamp() override;
    DataTypePtr getPartialAggregateStamp() override;
    DataTypePtr getFinalAggregateStamp() override;

    void inferStamp(const Schema& schema) override;

    WindowAggregationDescriptorPtr copy() override;
    MaxAggregationDescriptor(NodeFunctionPtr onField, NodeFunctionPtr asField);

    virtual ~MaxAggregationDescriptor() = default;

private:
    explicit MaxAggregationDescriptor(NodeFunctionFieldAccessPtr onField);
};
}
