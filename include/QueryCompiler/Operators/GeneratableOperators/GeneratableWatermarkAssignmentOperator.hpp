#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEWATERMARKASSIGNMENTOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEWATERMARKASSIGNMENTOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableWatermarkAssignmentOperator : public GeneratableOperator {
  public:
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;

    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    OperatorNodePtr copy() override;

  private:
    GeneratableWatermarkAssignmentOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                           Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEWATERMARKASSIGNMENTOPERATOR_HPP_
