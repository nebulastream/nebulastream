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


#ifndef NES_SRSWOROPERATORHANDLER_HPP
#define NES_SRSWOROPERATORHANDLER_HPP

#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::ASP {
/**
 * @brief OperatorHandler for a SRSWoR synopsis
 */
class SRSWoROperatorHandler : public Runtime::Execution::OperatorHandler {

  public:
    /**
     * @brief Getter for the stackRef
     * @return Returns the pointer to the stack
     */
    void* getStackRef();

    /**
     * @brief Initializes the stack
     * @param entrySize
     */
    void setup(uint64_t entrySize);

    /**
     * @brief Starts the operator handler.
     * @param pipelineExecutionContext
     * @param localStateVariableId
     * @param stateManager
     */
    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler.
     * @param pipelineExecutionContext
     */
    void stop(Runtime::QueryTerminationType terminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

  private:
    std::unique_ptr<Nautilus::Interface::Stack> stack;

};
} // namespace NES::ASP


#endif//NES_SRSWOROPERATORHANDLER_HPP
