#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEWINDOWOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEWINDOWOPERATOR_HPP_

#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class GeneratableWindowOperator : public WindowLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableWindowOperatorPtr create(WindowLogicalOperatorNodePtr);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

  private:
    explicit GeneratableWindowOperator(WindowDefinitionPtr windowDefinition);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEWINDOWOPERATOR_HPP_
