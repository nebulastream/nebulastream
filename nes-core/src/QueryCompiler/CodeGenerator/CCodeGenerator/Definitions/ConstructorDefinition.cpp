/*
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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/ConstructorDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/ConstructorDefinition.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

namespace NES::QueryCompilation {
ConstructorDefinition::ConstructorDefinition(std::string functionName, bool isExplicit)
    : name(std::move(functionName)), isExplicit(isExplicit) {}

ConstructorDefinitionPtr ConstructorDefinition::create(const std::string& functionName, bool isExplicit) {
    return std::make_shared<ConstructorDefinition>(functionName, isExplicit);
}

/// the following code creates a ctor like in the following
/// [explicit] nameOfTheCtor(parameter0, parameter1, ..., parameterN) : field0(initializerStatement0), ..., fieldM(initializerStatementM) {
///      variableDeclaration0
///      ...
///      variableDeclarationK
///      statement0
///      ..
///      statementQ
/// }
DeclarationPtr ConstructorDefinition::getDeclaration() {
    std::stringstream function;
    if (isExplicit) {
        function << "explicit";
    }
    function << " " << name << "(";
    for (uint64_t i = 0; i < parameters.size(); ++i) {
        function << parameters[i].getCode();
        if (i + 1 < parameters.size()) {
            function << ", ";
        }
    }
    function << ")";

    NES_ASSERT(initializerStatements.size() == fieldNameInitializers.size(), "wrong ctor config");
    if (initializerStatements.empty()) {
        function << " {";
    } else {
        function << " : ";
        for (uint64_t i = 0; i < initializerStatements.size(); ++i) {
            function << fieldNameInitializers[i] << "(" << initializerStatements[i]->getCode()->code_ << ")";
            if (i + 1 < initializerStatements.size()) {
                function << ", ";
            }
        }
        function << " {";
    }

    function << std::endl << "/* variable declarations */" << std::endl;
    for (auto& variablDeclaration : variablDeclarations) {
        function << variablDeclaration.getCode() << ";";
    }
    function << std::endl << "/* statements section */" << std::endl;
    for (auto& statement : statements) {
        function << statement->getCode()->code_ << ";";
    }
    function << "}";

    return ConstructorDeclaration::create(function.str());
}

ConstructorDefinitionPtr ConstructorDefinition::addInitializer(std::string&& fieldName, const StatementPtr& statement) {
    fieldNameInitializers.emplace_back(std::move(fieldName));
    initializerStatements.emplace_back(statement);
    return shared_from_this();
}

ConstructorDefinitionPtr ConstructorDefinition::addParameter(const VariableDeclaration& variableDeclaration) {
    parameters.emplace_back(variableDeclaration);
    return shared_from_this();
}
ConstructorDefinitionPtr ConstructorDefinition::addStatement(const StatementPtr& statement) {
    if (statement) {
        statements.emplace_back(statement);
    }
    return shared_from_this();
}

ConstructorDefinitionPtr ConstructorDefinition::addVariableDeclaration(const VariableDeclaration& variableDeclaration) {
    variablDeclarations.emplace_back(variableDeclaration);
    return shared_from_this();
}
}// namespace NES::QueryCompilation