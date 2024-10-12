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
 * The SumAggregationDescriptor aggregation calculates the running sum over the window.
 */
class SumAggregationDescriptor : public WindowAggregationDescriptor
{
public:
    virtual ~SumAggregationDescriptor() = default;

    /**
    * Factory method to creates a sum aggregation on a particular field.
    */
    static WindowAggregationDescriptorPtr on(const FunctionNodePtr& onField);

    static WindowAggregationDescriptorPtr create(FieldAccessFunctionNodePtr onField, FieldAccessFunctionNodePtr asField);

    /**
     * @brief Infers the stamp of the function given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    WindowAggregationDescriptorPtr copy() override;

    DataTypePtr getInputStamp() override;
    DataTypePtr getPartialAggregateStamp() override;
    DataTypePtr getFinalAggregateStamp() override;

private:
    explicit SumAggregationDescriptor(FieldAccessFunctionNodePtr onField);
    SumAggregationDescriptor(FunctionNodePtr onField, FunctionNodePtr asField);
};
} /// namespace NES::Windowing
