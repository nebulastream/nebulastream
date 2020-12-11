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
#ifndef NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DEFINITIONS_CONSTRUCTORDEFINITION_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DEFINITIONS_CONSTRUCTORDEFINITION_HPP_


#include <QueryCompiler/CCodeGenerator/CCodeGeneratorForwardRef.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

/**
 * @brief Definition for a constructor in the generated code.
 */
class ConstructorDefinition : public std::enable_shared_from_this<ConstructorDefinition> {

  public:
    ConstructorDefinition(const std::string& functionName, bool isExplicit);

    /**
     * @brief Factory to create the constructor definition.
     * @param functionName name of the constructor.
     * @return ConstructorDefinitionPtr
     */
    static ConstructorDefinitionPtr create(const std::string& functionName, bool isExplicit = false);

    /**
     * @brief Adds a parameter to the constructor
     * @param variableDeclaration VariableDeclaration
     * @return ConstructorDefinitionPtr
     */
    ConstructorDefinitionPtr addParameter(VariableDeclaration variableDeclaration);

    /**
     * @brief Adds a statement to the constructor body.
     * @param statement StatementPtr
     * @return FunctionDefinitionPtr
     */
    ConstructorDefinitionPtr addStatement(StatementPtr statement);

    /**
     * @brief Adds a variable declaration to this constructor.
     * @param variableDeclaration VariableDeclaration
     * @return ConstructorDefinitionPtr
     */
    ConstructorDefinitionPtr addVariableDeclaration(VariableDeclaration variableDeclaration);

    /**
     * @brief Adds a variable declaration to this constructor.
     * @param variableDeclaration VariableDeclaration
     * @return ConstructorDefinitionPtr
     */
    ConstructorDefinitionPtr addInitializer(std::string&& fieldName, StatementPtr statement);

    /**
     * @brief Creates the declaration of this constructor.
     * @return DeclarationPtr
     */
    DeclarationPtr getDeclaration();

  private:
    std::string name;
    std::vector<VariableDeclaration> parameters;
    std::vector<VariableDeclaration> variablDeclarations;
    std::vector<StatementPtr> statements;
    std::vector<std::string> fieldNameInitializers;
    std::vector<StatementPtr> initializerStatements;
    bool isExplicit;
};

}// namespace NES


#endif//NES_INCLUDE_QUERYCOMPILER_CCODEGENERATOR_DEFINITIONS_CONSTRUCTORDEFINITION_HPP_
