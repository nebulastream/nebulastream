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

#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_INFERMODELOPERATORHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_INFERMODELOPERATORHANDLER_HPP_
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/TensorflowAdapter.hpp>
#include <Windowing/JoinForwardRefs.hpp>

// todo create NES::InferModel namespace
namespace NES::Join {

/**
 * @brief Operator handler for inferModel.
 */
class InferModelOperatorHandler : public Runtime::Execution::OperatorHandler {
  public:
    InferModelOperatorHandler(std::string model);

    static InferModelOperatorHandlerPtr create(std::string model);

    ~InferModelOperatorHandler() override { NES_DEBUG("~InferModelOperatorHandler()"); }

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType, Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

  private:
    std::string model;
    TensorflowAdapterPtr tfAdapter;

  public:
    const std::string& getModel() const;
    const TensorflowAdapterPtr& getTensorflowAdapter() const;
};
}// namespace NES::InferModel

#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_INFERMODELOPERATORHANDLER_HPP_