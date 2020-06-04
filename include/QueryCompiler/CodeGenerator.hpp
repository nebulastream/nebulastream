#pragma once

#include "Operators/Operator.hpp"
#include <memory>

#include <API/Schema.hpp>
#include <API/Types/DataTypes.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <SourceSink/DataSink.hpp>

namespace NES {

class AttributeReference;
typedef std::shared_ptr<AttributeReference> AttributeReferencePtr;

class CodeGenerator;
typedef std::shared_ptr<CodeGenerator> CodeGeneratorPtr;

class PipelineStage;
typedef std::shared_ptr<PipelineStage> PipelineStagePtr;

class ExecutablePipeline;
typedef std::shared_ptr<ExecutablePipeline> ExecutablePipelinePtr;

// class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class CompilerArgs;
class CodeGenArgs;

class CompilerArgs {
};

class CodeGenArgs {
};

class CodeGenerator {
  public:
    CodeGenerator(const CodeGenArgs& args);
    // virtual bool addOperator(OperatorPtr) = 0;
    virtual bool generateCode(SchemaPtr schema, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const PredicatePtr& pred, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const AttributeFieldPtr field,
                              const PredicatePtr& pred,
                              const NES::PipelineContextPtr& context,
                              std::ostream& out) = 0;
    virtual bool generateCodeForSink(const SchemaPtr sink, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual bool generateCode(const WindowDefinitionPtr& window, const PipelineContextPtr& context, std::ostream& out) = 0;
    virtual ExecutablePipelinePtr compile(const CompilerArgs&, const GeneratedCodePtr& code) = 0;
    virtual ~CodeGenerator();

    CodeGenArgs args;
};

/** \brief factory method for creating a code generator */
CodeGeneratorPtr createCodeGenerator();
/** \brief factory method for creating a pipeline context */
const PipelineContextPtr createPipelineContext();

class GeneratedCode {
  public:
    GeneratedCode();
    std::vector<VariableDeclaration> variableDeclarations;
    std::vector<StatementPtr> variableInitStmts;
    std::shared_ptr<FOR> forLoopStmt;
    /* points to the current scope (compound statement)
   * to insert the code of the next operation,
   * important when multiple levels of nesting occur
   * due to loops (for(){ <cursor> }) or
   * if statements (if(..){ <cursor>}) */
    CompoundStatementPtr currentCodeInsertionPoint;
    std::vector<StatementPtr> cleanupStmts;
    StatementPtr returnStmt;
    std::shared_ptr<VariableDeclaration> varDeclarationRecordIndex;
    std::shared_ptr<VariableDeclaration> varDeclarationReturnValue;
    StructDeclaration structDeclaratonInputTuple;
    StructDeclaration structDeclarationResultTuple;
    VariableDeclaration varDeclarationInputBuffer;
    VariableDeclaration varDeclarationWindowManager;
    VariableDeclaration varDeclarationResultBuffer;
    VariableDeclaration varDeclarationExecutionContext;
    VariableDeclaration varDeclarationState;
    FunctionCallStatement tupleBufferGetNumberOfTupleCall;
    VariableDeclaration varDeclarationInputTuples;
    VariableDeclaration varDeclarationNumberOfResultTuples;
    std::vector<StructDeclaration> typeDeclarations;
    std::vector<DeclarationPtr> override_fields;
};

typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

//const StructDeclaration getStructDeclarationTupleBuffer();
//const StructDeclaration getStructDeclarationWindowState();
//const StructDeclaration getStructDeclarationTupleBuffer();
//const StructDeclaration getStructDeclarationWindowState();
const StructDeclaration getStructDeclarationFromSchema(const std::string struct_name, SchemaPtr schema);
}// namespace NES
