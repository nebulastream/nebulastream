#ifndef NES_GENERATABLEWATERMARKASSIGNEROPERATOR_HPP
#define NES_GENERATABLEWATERMARKASSIGNEROPERATOR_HPP

#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>

namespace NES {

class GeneratableWatermarkAssignerOperator: public WatermarkAssignerLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableWatermarkAssignerOperatorPtr create(WatermarkAssignerLogicalOperatorNodePtr watermarkAssignerLogicalOperatorNode, OperatorId id = UtilityFunctions::getNextOperatorId());

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
    GeneratableWatermarkAssignerOperator(const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor, OperatorId id);
};
}// namespace NES

#endif//NES_GENERATABLEWATERMARKASSIGNEROPERATOR_HPP
