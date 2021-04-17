#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFEREMIT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFEREMIT_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableBufferEmit : public GeneratableOperator {
  public:
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    static GeneratableOperatorPtr create(SchemaPtr outputSchema);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr outputSchema);
    const std::string toString() const override;
    OperatorNodePtr copy() override;
  private:
    GeneratableBufferEmit(OperatorId id, SchemaPtr outputSchema);
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFEREMIT_HPP_
