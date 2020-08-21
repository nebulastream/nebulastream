#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMERGEOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMERGEOPERATOR_HPP_

#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class GeneratableMergeOperator : public MergeLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableMergeOperatorPtr create(MergeLogicalOperatorNodePtr logicalMergeOperator);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

  private:
    GeneratableMergeOperator(SchemaPtr outputSchema);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMERGEOPERATOR_HPP_
