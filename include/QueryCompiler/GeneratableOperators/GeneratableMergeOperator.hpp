#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMERGEOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMERGEOPERATOR_HPP_

#include <Nodes/Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {

class GeneratableMergeOperator : public MergeLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableMergeOperatorPtr create(MergeLogicalOperatorNodePtr logicalMergeOperator);

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
    GeneratableMergeOperator(SchemaPtr outputSchema);
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEMERGEOPERATOR_HPP_
