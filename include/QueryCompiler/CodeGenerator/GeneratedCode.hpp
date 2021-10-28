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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_GENERATED_CODE_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_GENERATED_CODE_HPP_

#include <memory>
#include <vector>

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ForLoopStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES {
namespace QueryCompilation {

class GeneratedCode {
  public:
    GeneratedCode();
    std::vector<StatementPtr> setupCode;
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
    std::vector<StructDeclaration> structDeclarationInputTuples;
    StructDeclaration structDeclarationResultTuple;
    VariableDeclaration varDeclarationInputBuffer;
    VariableDeclaration varDeclarationNumOfInputTuples;
    VariableDeclaration varDeclarationResultBuffer;
    VariableDeclaration varDeclarationWorkerContext;
    VariableDeclaration varDeclarationExecutionContext;
    FunctionCallStatement tupleBufferGetNumberOfTupleCall;
    VariableDeclaration varDeclarationInputTuples;
    VariableDeclaration varDeclarationTuplePassesFilters;
    VariableDeclaration varDeclarationNumberOfResultTuples;
    std::vector<StructDeclaration> typeDeclarations;
    std::vector<DeclarationPtr> override_fields;
};
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_GENERATED_CODE_HPP_
