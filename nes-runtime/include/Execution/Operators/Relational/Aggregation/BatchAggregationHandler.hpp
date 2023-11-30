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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHAGGREGATIONHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHAGGREGATIONHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief The BatchAggregationHandler provides an operator handler to perform aggregations.
 * This operator handler, maintains an aggregate as a state.
 */
class BatchAggregationHandler : public Runtime::Execution::OperatorHandler,
                                public ::NES::detail::virtual_enable_shared_from_this<BatchAggregationHandler, false> {
    using State = int8_t*;

  public:
    /**
     * @brief Creates the operator handler.
     */
    BatchAggregationHandler();

    /**
     * @brief Initializes the thread local state for the aggregation operator
     * @param ctx PipelineExecutionContext
     * @param entrySize Size of the aggregated values in memory
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,

               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Returns the thread local state by a specific worker thread id
     * @param workerId
     * @return State
     */
    State getThreadLocalState(uint64_t workerId);

    ~BatchAggregationHandler() override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

  private:
    std::vector<State> threadLocalStateStores;
};
}// namespace NES::Runtime::Execution::Operators
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHAGGREGATIONHANDLER_HPP_
