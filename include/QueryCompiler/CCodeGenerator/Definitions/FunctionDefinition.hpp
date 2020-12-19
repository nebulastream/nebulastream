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

#include <QueryCompiler/CCodeGenerator/CCodeGeneratorForwardRef.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <memory>
#include <string>

namespace NES {

/**
 * @brief Definition for a function in the generated code.
 */
class FunctionDefinition : public std::enable_shared_from_this<FunctionDefinition> {

  public:
    FunctionDefinition(const std::string& functionName);

    /**
     * @brief Factory to create the function definition.
     * @param functionName name of the function.
     * @return FunctionDefinitionPtr
     */
    static FunctionDefinitionPtr create(const std::string& functionName);

    /**
     * @brief Sets the return type of this function.
     * @param returnType GeneratableDataTypePtr
     * @return FunctionDefinitionPtr
     */
    FunctionDefinitionPtr returns(GeneratableDataTypePtr returnType);

    /**
     * @brief Adds a parameter to the function
     * @param variableDeclaration VariableDeclaration
     * @return FunctionDefinitionPtr
     */
    FunctionDefinitionPtr addParameter(VariableDeclaration variableDeclaration);

    /**
     * @brief Adds a statement to the function body.
     * @param statement StatementPtr
     * @return FunctionDefinitionPtr
     */
    FunctionDefinitionPtr addStatement(StatementPtr statement);

    /**
     * @brief Adds a variable declaration to this function.
     * @param variableDeclaration VariableDeclaration
     * @return FunctionDefinitionPtr
     */
    FunctionDefinitionPtr addVariableDeclaration(VariableDeclaration variableDeclaration);

    /**
     * @brief Creates the declaration of this function.
     * @return DeclarationPtr
     */
    DeclarationPtr getDeclaration();

  private:
    std::string name;
    GeneratableDataTypePtr returnType;
    std::vector<VariableDeclaration> parameters;
    std::vector<VariableDeclaration> variablDeclarations;
    std::vector<StatementPtr> statements;
};

}// namespace NES
#endif// NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DECLARATIONS_FUNCTIONDEFINITION_HPP_
