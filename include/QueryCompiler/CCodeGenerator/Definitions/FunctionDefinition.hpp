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


#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_FUNCTIONDEFINITION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_FUNCTIONDEFINITION_HPP_

#include <memory>
#include <string>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGeneratorForwardRef.hpp>

namespace NES {

class FunctionDefinition : public std::enable_shared_from_this<FunctionDefinition>{

  public:
    static FunctionDefinitionPtr create(const std::string& functionName);
    FunctionDefinition(const std::string& functionName);

    FunctionDefinitionPtr returns(GeneratableDataTypePtr returnType);
    FunctionDefinitionPtr addParameter(VariableDeclaration variableDeclaration);
    FunctionDefinitionPtr addStatement(StatementPtr statement);
    FunctionDefinitionPtr addVariableDeclaration(VariableDeclaration variableDeclaration);
    DeclarationPtr getDeclaration();

  private:
    std::string name;
    GeneratableDataTypePtr returnType;
    std::vector<VariableDeclaration> parameters;
    std::vector<VariableDeclaration> variablDeclarations;
    std::vector<StatementPtr> statements;

};

}// namespace
#endif NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_FUNCTIONDEFINITION_HPP_
