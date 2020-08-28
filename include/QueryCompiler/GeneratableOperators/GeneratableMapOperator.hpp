#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMAPOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMAPOPERATOR_HPP_

#include <Nodes/Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class GeneratableMapOperator : public MapLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableMapOperatorPtr create(MapLogicalOperatorNodePtr mapLogicalOperatorNode);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

  private:
    GeneratableMapOperator(FieldAssignmentExpressionNodePtr mapExpression);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMAPOPERATOR_HPP_
