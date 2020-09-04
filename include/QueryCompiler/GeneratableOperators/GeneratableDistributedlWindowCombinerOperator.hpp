#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEDISTRIBUTEDLWINDOWCOMBINEROPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEDISTRIBUTEDLWINDOWCOMBINEROPERATOR_HPP_

#include <Nodes/Operators/LogicalOperators/WindowLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class GeneratableDistributedlWindowCombinerOperator : public WindowLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableDistributedlWindowCombinerOperatorPtr create(WindowLogicalOperatorNodePtr);

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
    explicit GeneratableDistributedlWindowCombinerOperator(WindowDefinitionPtr windowDefinition);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEDISTRIBUTEDLWINDOWCOMBINEROPERATOR_HPP_
