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
#include <QueryCompiler/CCodeGenerator/Declarations/ConstructorDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/ConstructorDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <Util/Logger.hpp>
#include <memory>
#include <sstream>

namespace NES {

ConstructorDefinition::ConstructorDefinition(const std::string& functionName, bool isExplicit)
    : name(functionName), isExplicit(isExplicit) {}

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
        if (i + 1 < parameters.size())
            function << ", ";
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
    for (uint64_t i = 0; i < variablDeclarations.size(); ++i) {
        function << variablDeclarations[i].getCode() << ";";
    }
    function << std::endl << "/* statements section */" << std::endl;
    for (uint64_t i = 0; i < statements.size(); ++i) {
        function << statements[i]->getCode()->code_ << ";";
    }
    function << "}";

    return ConstructorDeclaration::create(function.str());
}

ConstructorDefinitionPtr ConstructorDefinition::addInitializer(std::string&& fieldName, StatementPtr statement) {
    fieldNameInitializers.emplace_back(std::move(fieldName));
    initializerStatements.emplace_back(statement);
    return shared_from_this();
}

ConstructorDefinitionPtr ConstructorDefinition::addParameter(VariableDeclaration variableDeclaration) {
    parameters.emplace_back(variableDeclaration);
    return shared_from_this();
}
ConstructorDefinitionPtr ConstructorDefinition::addStatement(StatementPtr statement) {
    if (statement)
        statements.emplace_back(statement);
    return shared_from_this();
}

ConstructorDefinitionPtr ConstructorDefinition::addVariableDeclaration(VariableDeclaration variableDeclaration) {
    variablDeclarations.emplace_back(variableDeclaration);
    return shared_from_this();
}

}// namespace NES