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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGING_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGING_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/Aggregations/SliceMergingAction.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief GlobalSliceMerging operator that performs the merges pre-aggregated slices from the GlobalSlicePreAggregation operator
 * The slice merging operator is always the first element in a pipeline. Thus it acts like a scan and emits window to downstream operators.
 */
class NonKeyedSliceMerging : public Operator {
  public:
    /**
     * @brief Creates a GlobalSliceMerging operator
     * @param operatorHandlerIndex the index of the GlobalSliceMerging operator handler
     * @param aggregationFunctions the set of aggregation function that are performed on each slice merging step.
     */
    NonKeyedSliceMerging(uint64_t operatorHandlerIndex,
                         const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                         std::unique_ptr<SliceMergingAction> sliceMergingAction);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    /**
     * @brief Function to combine all pre-aggregated slices.
     * @param globalOperatorHandler reference to the window handler
     * @param sliceMergeTask reference to the slice merging task
     * @param endSliceTs the end timestamp
     * @return reference to the newly created slice
     */
    Value<MemRef> combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                           Value<MemRef>& sliceMergeTask) const;
    /**
     * @brief Function to emit a window to the downstream operator.
     * @param ctx execution context
     * @param windowStart start of the window
     * @param windowEnd end of the window
     */
    void
    emitWindow(ExecutionContext& ctx, Value<>& windowStart, Value<>& windowEnd, Value<>& sequenceNumber, Value<MemRef>&) const;

    const uint64_t operatorHandlerIndex;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    const std::unique_ptr<SliceMergingAction> sliceMergingAction;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGING_HPP_
