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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>

namespace NES::Windowing
{

class SamplingWoRDescriptor : public WindowAggregationDescriptor
{
public:
    static WindowAggregationDescriptorPtr on(const NodeFunctionPtr& onField);

    /// Creates a new SamplingWoRDescriptor
    /// @param onField field on which the aggregation should be performed
    /// @param asField function describing how the aggregated field should be called
    static WindowAggregationDescriptorPtr create(NodeFunctionFieldAccessPtr onField, NodeFunctionFieldAccessPtr asField);

    void inferStamp(const Schema& schema) override;

    WindowAggregationDescriptorPtr copy() override;

    DataTypePtr getInputStamp() override;
    DataTypePtr getPartialAggregateStamp() override;
    DataTypePtr getFinalAggregateStamp() override;

    virtual ~SamplingWoRDescriptor() = default;

private:
    explicit SamplingWoRDescriptor(NodeFunctionFieldAccessPtr onField);
    SamplingWoRDescriptor(NodeFunctionPtr onField, NodeFunctionPtr asField);
};
}

