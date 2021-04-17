#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEJOINBUILD_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEJOINBUILD_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableJoinBuildOperator : public GeneratableJoinOperator {
  public:
    GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler);
    GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  protected:
    GeneratableJoinBuildOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler);
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEJOINBUILD_HPP_
