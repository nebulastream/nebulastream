#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_

#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
namespace NES {

class GeneratableScanOperator : public OperatorNode, public GeneratableOperator {
  public:
    static GeneratableScanOperatorPtr create(SchemaPtr schema);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    explicit GeneratableScanOperator(SchemaPtr schema);
    SchemaPtr schema;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLESOURCOPERATOR_HPP_
