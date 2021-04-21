#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_EXECUTABLEOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_EXECUTABLEOPERATOR_HPP_
#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {

class ExecutableOperator : public UnaryOperatorNode {
  public:
    ExecutableOperator(OperatorId id, NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage);
    static OperatorNodePtr create(NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage);
    NodeEngine::Execution::ExecutablePipelineStagePtr getExecutablePipelineStage();
    const std::string toString() const override;
    OperatorNodePtr copy() override;
  private:
    NodeEngine::Execution::ExecutablePipelineStagePtr executablePipelineStage;
};

}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_EXECUTABLEOPERATOR_HPP_
