#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEFILTER_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEFILTER_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <QueryCompiler/LegacyExpression.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableFilterOperator : public GeneratableOperator {
  public:
    static GeneratableOperatorPtr create( SchemaPtr inputSchema, ExpressionNodePtr predicate);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, ExpressionNodePtr predicate);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
  private:
    GeneratableFilterOperator(OperatorId id, SchemaPtr inputSchema, ExpressionNodePtr predicate);
    ExpressionNodePtr predicate;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEFILTER_HPP_
