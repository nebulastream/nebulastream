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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGHANDLER_HPP_
#include <Execution/Operators/Streaming/Aggregations/SlidingWindowSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {
class State;
class NonKeyedSlice;
using NonKeyedSlicePtr = std::unique_ptr<NonKeyedSlice>;
class MultiOriginWatermarkProcessor;
/**
 * @brief The NonKeyedSliceMergingHandler merges thread local pre-aggregated slices for non-keyed
 * tumbling and sliding window aggregations.
 */
class NonKeyedSliceMergingHandler : public OperatorHandler {
  public:
    /**
     * @brief Constructor for the NonKeyedSliceMergingHandler
     * @param windowDefinition
     */
    NonKeyedSliceMergingHandler();

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Creates a new global slice for a specific slice merge task
     * @param sliceMergeTask SliceMergeTask
     * @return GlobalSlicePtr
     */
    NonKeyedSlicePtr createGlobalSlice(SliceMergeTask<NonKeyedSlice>* sliceMergeTask);
    const State* getDefaultState() const;

    ~NonKeyedSliceMergingHandler();

  private:
    uint64_t entrySize;
    std::unique_ptr<State> defaultState;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_NONKEYEDTIMEWINDOW_NONKEYEDSLICEMERGINGHANDLER_HPP_
