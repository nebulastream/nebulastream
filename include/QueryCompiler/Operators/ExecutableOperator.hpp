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
