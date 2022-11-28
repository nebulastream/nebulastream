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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERPHYSICALTONAUTILUSOPERATORS_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERPHYSICALTONAUTILUSOPERATORS_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <vector>

namespace NES {
namespace QueryCompilation {

/**
 * @brief This phase lowers a pipeline plan of physical operators into a pipeline plan of nautilus operators.
 * The lowering of individual operators is defined by the nautilus operator provider to improve extendability.
 */
class LowerPhysicalToNautilusOperators {
  public:
    /**
     * @brief Constructor to create a LowerPhysicalToGeneratableOperatorPhase
     */
    explicit LowerPhysicalToNautilusOperators();

    /**
     * @brief Create a LowerPhysicalToGeneratableOperatorPhase
     */
    static std::shared_ptr<LowerPhysicalToNautilusOperators> create();

    /**
     * @brief Applies the phase on a pipelined query plan.
     * @param pipelined query plan
     * @return PipelineQueryPlanPtr
     */
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr pipelinedQueryPlan, Runtime::NodeEnginePtr nodeEngine);

    /**
     * @brief Applies the phase on a pipelined and lower physical operator to generatable once.
     * @param pipeline
     * @return OperatorPipelinePtr
     */
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline, size_t bufferSize);

  private:
    std::shared_ptr<Runtime::Execution::Operators::Operator>
    lower(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
          std::shared_ptr<Runtime::Execution::Operators::Operator> parentOperator,
          PhysicalOperators::PhysicalOperatorPtr operatorPtr,
          size_t bufferSize,
          std::vector<Runtime::Execution::OperatorHandlerPtr>& operatorHandlers);
    std::shared_ptr<Runtime::Execution::Operators::Operator> lowerScan(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
                                                                       PhysicalOperators::PhysicalOperatorPtr sharedPtr,
                                                                       size_t bufferSize);
    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerEmit(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
              PhysicalOperators::PhysicalOperatorPtr sharedPtr,
              size_t bufferSize);
    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerFilter(Runtime::Execution::PhysicalOperatorPipeline& pipeline, PhysicalOperators::PhysicalOperatorPtr sharedPtr);
    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerMap(Runtime::Execution::PhysicalOperatorPipeline& pipeline, PhysicalOperators::PhysicalOperatorPtr sharedPtr);
    std::shared_ptr<Runtime::Execution::Operators::ExecutableOperator>
    lowerThresholdWindow(Runtime::Execution::PhysicalOperatorPipeline& pipeline,
                         PhysicalOperators::PhysicalOperatorPtr sharedPtr,
                         uint64_t handlerIndex);
    std::shared_ptr<Runtime::Execution::Expressions::Expression> lowerExpression(ExpressionNodePtr expressionNode);
};
}// namespace QueryCompilation
}// namespace NES
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_LOWERPHYSICALTONAUTILUSOPERATORS_HPP_
