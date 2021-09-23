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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATOR_PHYSICALOPERATOR_CEP_CEPOPERATORHANDLER_CEPOPERATORHANDLER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATOR_PHYSICALOPERATOR_CEP_CEPOPERATORHANDLER_CEPOPERATORHANDLER_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalCEPIterationOperator.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <State/StateManager.hpp>
#include <Windowing/CEPForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::CEP {
/**
 * @brief Operator handler for cep operators
 */
class CEPOperatorHandler : public Runtime::Execution::OperatorHandler {
  public:
    CEPOperatorHandler() = default;

    /**
     * @brief Factory to create new CEPOperatorHandler
     * @return CEPOperatorHandlerPtr
     */
    static CEPOperatorHandlerPtr create();

    /**
    * @brief Starts cep handler
     * @param pipelineExecutionContext pointer to the current pipeline execution context
     * @param stateManager point to the current state manager
     * @param localStateVariableId
    */
    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
    * @brief Stops cep handler
     * @param pipelineExecutionContext pointer to the current pipeline execution context
    */
    void stop(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

    ~CEPOperatorHandler() override { NES_DEBUG("~CEPOperatorHandler()"); }

    [[maybe_unused]] void incrementCounter();

    [[maybe_unused]] uint64_t getCounter();

    void clearCounter();

  private:
    uint64_t counter;
};
}// namespace NES::CEP

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATOR_PHYSICALOPERATOR_CEP_CEPOPERATORHANDLER_CEPOPERATORHANDLER_HPP_
