#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEFILTEROPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEFILTEROPERATOR_HPP_

#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class GeneratableFilterOperator : public FilterLogicalOperatorNode, public GeneratableOperator {

  public:

    /**
     * @brief Create sharable instance of GeneratableFilterOperator
     * @param filterLogicalOperator: the filter logical operator
     * @param id: the operator id if not provided then next available operator id is used.
     * @return instance of GeneratableFilterOperator
     */
    static GeneratableFilterOperatorPtr create(FilterLogicalOperatorNodePtr filterLogicalOperator, OperatorId id = UtilityFunctions::getNextOperatorId());

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
    GeneratableFilterOperator(const ExpressionNodePtr filterExpression, OperatorId id);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEFILTEROPERATOR_HPP_
