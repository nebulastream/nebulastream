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
#ifndef NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_NAUTILUSPIPELINEOPERATOR_HPP_
#define NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_NAUTILUSPIPELINEOPERATOR_HPP_
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES::QueryCompilation {

/**
 * @brief A nautilus pipeline, that can be stored in a query plan.
 */
class NautilusPipelineOperator : public UnaryOperatorNode {
  public:
    /**
     * @brief Creates a new nautilus pipeline operator, which captures a pipeline stage and a set of operators.
     * @return PhysicalOperatorPipeline for nautilus.
     */
    static OperatorNodePtr create(std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> nautilusPipeline,
                                  std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers);

    std::string toString() const override;
    OperatorNodePtr copy() override;
    std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> getNautilusPipeline();

    /**
     * @brief get a vector of operator handlers
     * @return operator handler
     */
    std::vector<Runtime::Execution::OperatorHandlerPtr> getOperatorHandlers();

  private:
    NautilusPipelineOperator(OperatorId id,
                             std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> nautilusPipeline,
                             std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers);
    std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> nautilusPipeline;
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
};

}// namespace NES::QueryCompilation

#endif  // NES_RUNTIME_INCLUDE_QUERYCOMPILER_OPERATORS_NAUTILUSPIPELINEOPERATOR_HPP_
