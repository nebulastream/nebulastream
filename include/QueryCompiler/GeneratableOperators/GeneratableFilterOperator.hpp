#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEFILTEROPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEFILTEROPERATOR_HPP_

#include <Nodes/Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class GeneratableFilterOperator : public FilterLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableFilterOperatorPtr create(FilterLogicalOperatorNodePtr filterLogicalOperator);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;

  private:
    GeneratableFilterOperator(const ExpressionNodePtr filterExpression);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEFILTEROPERATOR_HPP_
