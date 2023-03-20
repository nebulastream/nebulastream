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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINHANDLER_HPP_
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>
namespace NES::Runtime::Execution::Operators {

class BatchJoinHandler : public Runtime::Execution::OperatorHandler,
                         public detail::virtual_enable_shared_from_this<BatchJoinHandler, false> {

  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     */
    BatchJoinHandler();

    /**
     * @brief Initializes the thread local state for the window operator
     * @param ctx PipelineExecutionContext
     * @param entrySize Size of the aggregated values in memory
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize, uint64_t keySize, uint64_t valueSize);

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
    Nautilus::Interface::Stack* getThreadLocalState(uint64_t workerId);
    Nautilus::Interface::ChainedHashMap* mergeState();
    Nautilus::Interface::ChainedHashMap* getGlobalHashMap();

    ~BatchJoinHandler();

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

  private:
    std::vector<std::unique_ptr<Nautilus::Interface::Stack>> threadLocalStateStores;
    std::unique_ptr<Nautilus::Interface::ChainedHashMap> globalMap;
    uint64_t keySize;
    uint64_t valueSize;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINHANDLER_HPP_
