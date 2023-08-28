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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_INFERMODELOPERATORHANDLER_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_INFERMODELOPERATORHANDLER_HPP_
#include <Execution/Operators/Streaming/InferModel/InferenceAdapter.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Reconfigurable.hpp>

namespace NES::InferModel {

class InferModelOperatorHandler;
using InferModelOperatorHandlerPtr = std::shared_ptr<InferModelOperatorHandler>;

/**
 * @brief Operator handler for inferModel.
 */
class InferModelOperatorHandler : public Runtime::Execution::OperatorHandler {
  public:
    explicit InferModelOperatorHandler(const std::string& model);

    static InferModelOperatorHandlerPtr create(const std::string& model);

    ~InferModelOperatorHandler() override = default;
    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext,
               Runtime::StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

    const std::string& getModel() const;

    NES::Runtime::Execution::Operators::InferenceAdapter* getTensorflowAdapter() const;

  private:
    std::string model;
    NES::Runtime::Execution::Operators::InferenceAdapterPtr adapter;
};
}// namespace NES::InferModel

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_INFERMODELOPERATORHANDLER_HPP_
