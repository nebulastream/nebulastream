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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICEPREAGGREGATIONHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICEPREAGGREGATIONHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>
namespace NES::Runtime::Execution::Operators {


/**
 * @brief The GlobalThreadLocalPreAggregationOperatorHandler provides an operator handler to perform slice-based pre-aggregation
 * of global non-keyed tumbling and sliding windows.
 * This operator handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer triggerThreadLocalState is called, which checks if the thread-local slice store should be triggered.
 * This is decided by the current watermark timestamp.
 */
class BatchAggregationHandler : public Runtime::Execution::OperatorHandler,
                                         public detail::virtual_enable_shared_from_this<BatchAggregationHandler, false> {
    using inherited0 = detail::virtual_enable_shared_from_this<BatchAggregationHandler, false>;
    using inherited1 = Runtime::Reconfigurable;
    using State = int8_t*;
  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     */
    BatchAggregationHandler();

    /**
     * @brief Initializes the thread local state for the window operator
     * @param ctx PipelineExecutionContext
     * @param entrySize Size of the aggregated values in memory
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler and triggers all in flight slices.
     * @param pipelineExecutionContext pipeline execution context
     */
    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

     /**
     * @brief Returns the thread local slice store by a specific worker thread id
     * @param workerId
     * @return GlobalThreadLocalSliceStore
     */
    State getThreadLocalState(uint64_t workerId);

    ~BatchAggregationHandler();

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;
  private:
    std::vector<State> threadLocalStateStores;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_GLOBALSLICEPREAGGREGATIONHANDLER_HPP_
