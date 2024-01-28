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

#ifndef NES_OPERATORHANDLER_HPP
#define NES_OPERATORHANDLER_HPP
#include <cstdint>
#include <memory>

//Prevent Existing OperatorHandler Header from being evaluated
#define NES_RUNTIME_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_

namespace NES::Unikernel {
class UnikernelPipelineExecutionContext;
}
namespace NES::Runtime {
class StateManager;
using StateManagerPtr = std::shared_ptr<StateManager>;
class QueryTerminationType;
}// namespace NES::Runtime
namespace NES::Runtime::Execution {
using PipelineExecutionContext = NES::Unikernel::UnikernelPipelineExecutionContext;
using PipelineExecutionContextPtr = NES::Unikernel::UnikernelPipelineExecutionContext*;
/**
 * @brief Interface to handle specific operator state.
 */
class OperatorHandler {
  public:
    /**
     * @brief Default constructor
     */
    OperatorHandler() = default;

    virtual ~OperatorHandler() = default;

    /**
     * @brief Starts the operator handler.
     * @param pipelineExecutionContext
     * @param localStateVariableId
     * @param stateManager
     */
    virtual void start(PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) = 0;

    /**
     * @brief Stops the operator handler.
     * @param pipelineExecutionContext
     */
    virtual void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) = 0;
};

}// namespace NES::Runtime::Execution

#endif//NES_OPERATORHANDLER_HPP
