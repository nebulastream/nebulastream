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

    /**
    * @brief Produce function, which calls the child produce function and brakes pipelines if necessary.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
    * @brief Consume function, which generates code for the processing and calls the parent consume function.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
    * @brief To string method for the operator.
    * @return string
    */
    const std::string toString() const override;

  private:
    GeneratableFilterOperator(const ExpressionNodePtr filterExpression);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEFILTEROPERATOR_HPP_
