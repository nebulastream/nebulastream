#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
namespace NES {

class GeneratableScanOperator : public OperatorNode, public GeneratableOperator {
  public:
    /**
     * @brief Create sharable instance of GeneratableScanOperator
     * @param schema: the schema of the input records
     * @param id: the operator id if not provided then next available operator id is used.
     * @return instance of GeneratableScanOperator
     */
    static GeneratableScanOperatorPtr create(SchemaPtr schema, OperatorId id = UtilityFunctions::getNextOperatorId());

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

    /**
    * Create a shallow copy of the operator by copying its operator properties but not its children or parent operator tree.
    * @return shallow copy of the operator
    */
    OperatorNodePtr copy() override;

  private:
    explicit GeneratableScanOperator(SchemaPtr schema, OperatorId id);
    SchemaPtr schema;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_
