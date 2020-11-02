#ifndef ES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEDISTRIBUTEDLWINDOWSLICECREATIONOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEDISTRIBUTEDLWINDOWSLICECREATIONOPERATOR_HPP_

#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>

namespace NES {

class GeneratableSlicingWindowOperator : public GeneratableWindowOperator {
  public:
    /**
     * @brief Create sharable instance of GeneratableSlicingWindowOperator
     * @param windowLogicalOperator: the window logical operator
     * @param id: the operator id if not provided then next available operator id is used.
     * @return instance of GeneratableSlicingWindowOperator
     */
    static GeneratableDistributedlWindowSliceCreationOperatorPtr create(Windowing::LogicalWindowDefinitionPtr windowDefinition, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id = UtilityFunctions::getNextOperatorId());

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
    explicit GeneratableSlicingWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEDISTRIBUTEDLWINDOWSLICECREATIONOPERATOR_HPP_
