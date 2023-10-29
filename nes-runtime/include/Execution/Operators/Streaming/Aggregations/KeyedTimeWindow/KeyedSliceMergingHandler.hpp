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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDSLICEMERGINGHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDSLICEMERGINGHANDLER_HPP_
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {
class State;
class KeyedSlice;
using KeyedSlicePtr = std::unique_ptr<KeyedSlice>;
class KeyedSliceStaging;

/**
 * @brief The GlobalSliceMergingHandler merges thread local pre-aggregated slices for global
 * tumbling and sliding window aggregations.
 */
class KeyedSliceMergingHandler : public OperatorHandler {

  public:
    static const uint64_t DEFAULT_NUMBER_OF_KEYS = 1000;
    /**
     * @brief Constructor for the KeyedSliceMergingHandler
     * @param windowDefinition
     */
    KeyedSliceMergingHandler();

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t keySize, uint64_t valueSize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;
    /**
     * @brief Creates a new global slice for a specific slice merge task
     * @param sliceMergeTask SliceMergeTask
     * @return GlobalSlicePtr
     */
    KeyedSlicePtr createGlobalSlice(SliceMergeTask<KeyedSlice>* sliceMergeTask, uint64_t numberOfKeys = DEFAULT_NUMBER_OF_KEYS);
    ~KeyedSliceMergingHandler() override;

  private:
    uint64_t keySize;
    uint64_t valueSize;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDSLICEMERGINGHANDLER_HPP_
