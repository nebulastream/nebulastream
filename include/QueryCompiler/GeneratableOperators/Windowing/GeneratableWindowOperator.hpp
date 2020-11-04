#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_GENERATABLEWINDOWOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_GENERATABLEWINDOWOPERATOR_HPP_

#include <Operators/LogicalOperators/Windowing/WindowLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES::Windowing {
class AbstractWindowHandler;
typedef std::shared_ptr<AbstractWindowHandler> AbstractWindowHandlerPtr;
}// namespace NES::Windowing

namespace NES {
class GeneratableWindowAggregation;
typedef std::shared_ptr<GeneratableWindowAggregation> GeneratableWindowAggregationPtr;

class GeneratableWindowOperator : public WindowLogicalOperatorNode, public GeneratableOperator {
  public:
    /**
     * @brief Produce function, which calls the child produce function and brakes pipelines if necessary.
     * @param codegen a pointer to the code generator.
     * @param context a pointer to the current pipeline context.
     */
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context) override = 0;

    /**
     * @brief Consume function, which generates code for the processing and calls the parent consume function.
     * @param codegen a pointer to the code generator.
     * @param context a pointer to the current pipeline context.
     */
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context) override = 0;

    /**
     * @brief To string method for the operator.
     * @return string
     */
    [[nodiscard]] virtual const std::string toString() const = 0;

    Windowing::AbstractWindowHandlerPtr createWindowHandler();

  protected:
    explicit GeneratableWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id);
    GeneratableWindowAggregationPtr generatableWindowAggregation;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_WINDOWING_GENERATABLEWINDOWOPERATOR_HPP_
