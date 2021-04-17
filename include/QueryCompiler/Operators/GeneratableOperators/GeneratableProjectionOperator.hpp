#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEPROJECT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEPROJECT_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableProjectionOperator : public GeneratableOperator {
  public:
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         std::vector<ExpressionNodePtr> expressions);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         std::vector<ExpressionNodePtr> expressions);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    GeneratableProjectionOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                  std::vector<ExpressionNodePtr> expressions);
    std::vector<ExpressionNodePtr> expressions;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEPROJECT_HPP_
