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

#ifndef NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWINFERENCEOPERATORHANDLER_HPP_
#define NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWINFERENCEOPERATORHANDLER_HPP_

#include <Execution/Operators/MEOS/MeosOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Runtime::Execution::Operators {

class MeosOperatorHandler;
using MeosOperatorHandlerPtr = std::shared_ptr<MeosOperatorHandler>;

/**
 * @brief Operator handler for inferModel.
 */
class MeosOperatorHandler : public OperatorHandler {
  public:
    explicit MeosOperatorHandler(const std::string& function);

    static MeosOperatorHandlerPtr create(const std::string& function);

    ~MeosOperatorHandler() override = default;

    void start(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext, uint32_t localStateVariableId) override;

    void stop(Runtime::QueryTerminationType queryTerminationType,
              Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override;

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override;

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override;

    const std::string& getFunction() const;

    const MeosOperatorHandlerPtr& getMeosAdapter() const;

  private:
    std::string function;
    MeosOperatorHandlerPtr meosAdapter;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_PLUGINS_TENSORFLOW_INCLUDE_EXECUTION_OPERATORS_TENSORFLOW_TENSORFLOWINFERENCEOPERATORHANDLER_HPP_
