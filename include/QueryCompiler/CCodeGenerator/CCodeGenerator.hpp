#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_

#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/TypeCastExprStatement.hpp>

namespace NES{

class CCodeGenerator : public CodeGenerator {
  public:
    CCodeGenerator();
    static CodeGeneratorPtr create();
    virtual bool generateCode(SchemaPtr schema, const PipelineContextPtr& context,
                              std::ostream& out) override;
    virtual bool generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) override;
    virtual bool generateCode(const AttributeFieldPtr field,
                              const PredicatePtr& pred,
                              const NES::PipelineContextPtr& context,
                              std::ostream& out) override;
    virtual bool generateCode(const DataSinkPtr& sink, const PipelineContextPtr& context, std::ostream& out) override;
    virtual bool generateCode(const WindowDefinitionPtr& window,
                              const PipelineContextPtr& context,
                              std::ostream& out) override;
    ExecutablePipelinePtr compile(const CompilerArgs&, const GeneratedCodePtr& code) override;
    ~CCodeGenerator() override;

  private:
    BinaryOperatorStatement getBuffer(VariableDeclaration tupleBufferVariable);
    TypeCastExprStatement getTypedBuffer(VariableDeclaration tupleBufferVariable, StructDeclaration structDeclaraton);
    BinaryOperatorStatement getBufferSize(VariableDeclaration tupleBufferVariable);
    BinaryOperatorStatement setNumberOfTuples(VariableDeclaration tupleBufferVariable,
                                              VariableDeclaration numberOfResultTuples);
    BinaryOperatorStatement allocateTupleBuffer(VariableDeclaration pipelineContext);
    BinaryOperatorStatement emitTupleBuffer(VariableDeclaration pipelineContext,
                                            VariableDeclaration tupleBufferVariable);
    void generateTupleBufferSpaceCheck(const PipelineContextPtr& context,
                                       VariableDeclaration varDeclResultTuple,
                                       StructDeclaration structDeclarationResultTuple);
};


}

#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_CCODEGENERATOR_HPP_
