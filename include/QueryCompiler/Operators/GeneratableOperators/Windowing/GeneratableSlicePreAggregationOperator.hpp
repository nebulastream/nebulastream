#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLESLICEPREAGGREGATIONOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLESLICEPREAGGREGATIONOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>

#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableSlicePreAggregationOperator : public GeneratableWindowOperator {
  public:
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         Windowing::WindowOperatorHandlerPtr operatorHandler, GeneratableWindowAggregationPtr windowAggregation);
    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         Windowing::WindowOperatorHandlerPtr operatorHandler, GeneratableWindowAggregationPtr windowAggregation);

    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    GeneratableSlicePreAggregationOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                           Windowing::WindowOperatorHandlerPtr operatorHandler, GeneratableWindowAggregationPtr windowAggregation);
    GeneratableWindowAggregationPtr windowAggregation;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLESLICEPREAGGREGATIONOPERATOR_HPP_
