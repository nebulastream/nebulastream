/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_
#define NES_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace Runtime {
namespace Execution {

/**
 * @brief Interface to handle specific operator state.
 */
class OperatorHandler : public Reconfigurable {
  public:
    OperatorHandler() = default;

    /**
     * @brief Starts the operator handler.
     * @param pipelineExecutionContext
     * @param localStateVariableId
     * @param stateManager
     */
    virtual void
    start(PipelineExecutionContextPtr pipelineExecutionContext, StateManagerPtr stateManager, uint32_t localStateVariableId) = 0;

    /**
     * @brief Stops the operator handler.
     * @param pipelineExecutionContext
     */
    virtual void stop(PipelineExecutionContextPtr pipelineExecutionContext) = 0;
};

}// namespace Execution
}// namespace Runtime
}// namespace NES

#endif  // NES_INCLUDE_RUNTIME_EXECUTION_OPERATORHANDLER_HPP_
