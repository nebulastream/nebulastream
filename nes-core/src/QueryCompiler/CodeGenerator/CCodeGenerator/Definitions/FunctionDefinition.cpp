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

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <API/AttributeField.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <sstream>
namespace NES::QueryCompilation {
class StructBuilder {
  public:
    static StructBuilder create(const std::string& struct_name);
    StructBuilder& addField(AttributeFieldPtr attr);
    StructDeclaration build();
};

class StatementBuilder {
  public:
    static StatementBuilder create(const std::string& struct_name);
};

FunctionDefinition::FunctionDefinition(std::string functionName) : name(std::move(functionName)) {}

FunctionDefinitionPtr FunctionDefinition::create(const std::string& functionName) {
    return std::make_shared<FunctionDefinition>(functionName);
}

DeclarationPtr FunctionDefinition::getDeclaration() {
    std::stringstream function;
    if (!returnType) {
        function << "void";
    } else {
        function << returnType->getCode()->code_;
    }
    function << " " << name << "(";
    for (uint64_t i = 0; i < parameters.size(); ++i) {
        function << parameters[i].getCode();
        if (i + 1 < parameters.size()) {
            function << ", ";
        }
    }
    function << "){";

    function << std::endl << "/* variable declarations */" << std::endl;
    for (auto& variablDeclaration : variablDeclarations) {
        function << variablDeclaration.getCode() << ";";
    }
    function << std::endl << "/* statements section */" << std::endl;
    for (auto& statement : statements) {
        function << statement->getCode()->code_ << ";";
    }
    function << "}";

    return FunctionDeclaration::create(function.str());
}

FunctionDefinitionPtr FunctionDefinition::returns(GeneratableDataTypePtr type) {
    returnType = std::move(type);
    return shared_from_this();
}

FunctionDefinitionPtr FunctionDefinition::addParameter(const VariableDeclaration& variableDeclaration) {
    parameters.emplace_back(variableDeclaration);
    return shared_from_this();
}
FunctionDefinitionPtr FunctionDefinition::addStatement(const StatementPtr& statement) {
    if (statement) {
        statements.emplace_back(statement);
    }
    return shared_from_this();
}

FunctionDefinitionPtr FunctionDefinition::addVariableDeclaration(const VariableDeclaration& variableDeclaration) {
    variablDeclarations.emplace_back(variableDeclaration);
    return shared_from_this();
}
}// namespace NES::QueryCompilation
