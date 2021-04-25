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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_EXECUTABLEOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_EXECUTABLEOPERATOR_HPP_
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {

class ExecutableOperator : public UnaryOperatorNode {
  public:
    ExecutableOperator(OperatorId id, NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage,
                       std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers);
    static OperatorNodePtr create(NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage,
                                  std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers);
    NodeEngine::Execution::ExecutablePipelineStagePtr getExecutablePipelineStage();
    std::vector<NodeEngine::Execution::OperatorHandlerPtr> getOperatorHandlers();
    const std::string toString() const override;
    OperatorNodePtr copy() override;
  private:
    NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage;
    std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers;
};

}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_EXECUTABLEOPERATOR_HPP_
