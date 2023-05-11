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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORTHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORTHANDLER_HPP_

#include <Runtime/Execution/OperatorHandler.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief TODO
 **/
class SortHandler : public Runtime::Execution::OperatorHandler,
                         public NES::detail::virtual_enable_shared_from_this<SortHandler, false> {

  public:
    /**
     * @brief Creates the operator handler for the sort operator.
     */
    SortHandler();

    /**
     * @brief Destroys the operator handler for the sort operator.
     */
    ~SortHandler();

    /**
     * @brief Initializes the thread local state for the sort operator
     * @param ctx PipelineExecutionContext
     * @param entrySize size of the entry
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize);

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    // TODO: do we need this?
    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

    /**
     * @brief Returns the thread local state for a  specific worker thread id
     * @param workerId
     * @return Nautilus::Interface::Stack*
     */
    Nautilus::Interface::Stack* getThreadLocalState(uint64_t workerId);

  private:
    std::vector<std::unique_ptr<Nautilus::Interface::Stack>> threadLocalStateStores;
};
}// namespace NES::Runtime::Execution::Operators
#endif //NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SORTHANDLER_HPP_
