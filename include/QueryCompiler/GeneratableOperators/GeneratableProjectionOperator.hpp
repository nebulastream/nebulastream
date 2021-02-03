
#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEPROJECTIONOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEPROJECTIONOPERATOR_HPP_

#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperatorForwardRef.hpp>

namespace NES{

/**
 * @brief Generatable projection operator
 */
class GeneratableProjectionOperator : public ProjectionLogicalOperatorNode, public GeneratableOperator {
  public:
    /**
    * @brief Create sharable instance of GeneratableProjectOperator
    * @param projectLogicalOperator: the project logical operator
    * @param id: the operator id if not provided then next available operator id is used.
    * @return instance of GeneratableMapOperator
    */
    static GeneratableProjectionOperatorPtr create(ProjectionLogicalOperatorNodePtr projectLogicalOperator,
                                            OperatorId id = UtilityFunctions::getNextOperatorId());

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
    GeneratableProjectionOperator(std::vector<ExpressionNodePtr> expressions, OperatorId id);

};

}

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEPROJECTIONOPERATOR_HPP_
