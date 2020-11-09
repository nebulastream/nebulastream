/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATEDCODE_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATEDCODE_HPP_

#include <memory>
#include <vector>

#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ForLoopStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
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
    VariableDeclaration varDeclarationResultBuffer;
    VariableDeclaration varDeclarationWorkerContext;
    VariableDeclaration varDeclarationExecutionContext;
    FunctionCallStatement tupleBufferGetNumberOfTupleCall;
    VariableDeclaration varDeclarationInputTuples;
    VariableDeclaration varDeclarationNumberOfResultTuples;
    std::vector<StructDeclaration> typeDeclarations;
    std::vector<DeclarationPtr> override_fields;
};

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATEDCODE_HPP_
