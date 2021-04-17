#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEMAP_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEMAP_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableMapOperator : public GeneratableOperator {
  public:
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema,FieldAssignmentExpressionNodePtr mapExpression);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,FieldAssignmentExpressionNodePtr mapExpression);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;
  private:
    GeneratableMapOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,FieldAssignmentExpressionNodePtr mapExpression);
    const FieldAssignmentExpressionNodePtr mapExpression;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEMAP_HPP_
