#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFERESCAN_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFERESCAN_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableBufferScan : public GeneratableOperator {
  public:
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    const std::string toString() const override;
    static GeneratableOperatorPtr create(SchemaPtr inputSchema);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema);
    OperatorNodePtr copy() override;

  private:
    GeneratableBufferScan(OperatorId id, SchemaPtr inputSchema);
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFERESCAN_HPP_
