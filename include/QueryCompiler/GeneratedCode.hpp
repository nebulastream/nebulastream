#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATEDCODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATEDCODE_HPP_

#include <vector>
#include <memory>

#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ForLoopStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/Compiler.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

class GeneratedCode;
typedef std::shared_ptr<GeneratedCode> GeneratedCodePtr;

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



}



#endif//NES_INCLUDE_QUERYCOMPILER_GENERATEDCODE_HPP_
