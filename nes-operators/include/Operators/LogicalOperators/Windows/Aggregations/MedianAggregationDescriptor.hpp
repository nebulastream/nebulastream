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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_MEDIANAGGREGATIONDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_MEDIANAGGREGATIONDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Windows/Aggregations/WindowAggregationDescriptor.hpp>

namespace NES::Windowing {
/**
 * @brief
 * The MedianAggregationDescriptor aggregation calculates the median over the window.
 */
class MedianAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
    * @brief Factory method to creates a median aggregation on a particular field.
    * @param onField field on which the aggregation should be performed
    */
    static WindowAggregationDescriptorPtr on(const ExpressionNodePtr& onField);

    /**
     * @brief Factory method to creates a median aggregation on a particular field.
     * @param onField field on which the aggregation should be performed
     * @param asField expression describing how the aggregated field should be called
     */
    static WindowAggregationDescriptorPtr create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(SchemaPtr schema) override;

    WindowAggregationDescriptorPtr copy() override;

    DataTypePtr getInputStamp() override;
    DataTypePtr getPartialAggregateStamp() override;
    DataTypePtr getFinalAggregateStamp() override;

    virtual ~MedianAggregationDescriptor() = default;

  private:
    /**
     * @brief Creates a new MedianAggregationDescriptor
     * @param onField field on which the aggregation should be performed
     */
    explicit MedianAggregationDescriptor(FieldAccessExpressionNodePtr onField);

    /**
     * @brief Creates a new MedianAggregationDescriptor
     * @param onField field on which the aggregation should be performed
     * @param asField expression describing how the aggregated field should be called
     */
    MedianAggregationDescriptor(ExpressionNodePtr onField, ExpressionNodePtr asField);
};
}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_MEDIANAGGREGATIONDESCRIPTOR_HPP_
