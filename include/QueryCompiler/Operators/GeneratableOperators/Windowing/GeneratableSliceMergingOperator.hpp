#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLESLICEMERGINGOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLESLICEMERGINGOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableSliceMergingOperator : public GeneratableWindowOperator {
  public:
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr operatorHandler);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr operatorHandler);

    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    GeneratableSliceMergingOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr operatorHandler);


};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLESLICEMERGINGOPERATOR_HPP_
