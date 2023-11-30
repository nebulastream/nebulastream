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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHKEYEDAGGREGATIONHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHKEYEDAGGREGATIONHANDLER_HPP_
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>
namespace NES::Runtime::Execution::Operators {

class MultiOriginWatermarkProcessor;
class KeyedSliceStaging;
class KeyedThreadLocalSliceStore;
class State;
/**
 * @brief The BatchKeyedAggregationHandler provides an operator handler to perform keyed aggregations.
 * This operator handler, maintains a hash-map for each worker thread and provides them for the aggregation.
 */
class BatchKeyedAggregationHandler : public Runtime::Execution::OperatorHandler,
                                     public NES::detail::virtual_enable_shared_from_this<BatchKeyedAggregationHandler, false> {
  public:
    /**
     * @brief Creates the operator handler.
     */
    BatchKeyedAggregationHandler();

    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t keySize, uint64_t valueSize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,

               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Returns the thread local slice store by a specific worker thread id
     * @param workerId
     * @return GlobalThreadLocalSliceStore
     */
    Nautilus::Interface::ChainedHashMap* getThreadLocalStore(uint64_t workerId);

    ~BatchKeyedAggregationHandler() override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

  private:
    std::vector<std::unique_ptr<Nautilus::Interface::ChainedHashMap>> threadLocalSliceStores;
};
}// namespace NES::Runtime::Execution::Operators
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHKEYEDAGGREGATIONHANDLER_HPP_
