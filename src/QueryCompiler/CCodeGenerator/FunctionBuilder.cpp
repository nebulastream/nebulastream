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
#include <vector>

#include <API/AttributeField.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <sstream>
namespace NES {

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

FunctionBuilder::FunctionBuilder(const std::string& function_name) : name(function_name) {}

FunctionBuilder FunctionBuilder::create(const std::string& function_name) { return FunctionBuilder(function_name); }

FunctionDeclaration FunctionBuilder::build() {
    std::stringstream function;
    if (!returnType) {
        function << "void";
    } else {
        function << returnType->getCode()->code_;
    }
    function << " " << name << "(";
    for (size_t i = 0; i < parameters.size(); ++i) {
        function << parameters[i].getCode();
        if (i + 1 < parameters.size())
            function << ", ";
    }
    function << "){";

    function << std::endl
             << "/* variable declarations */" << std::endl;
    for (size_t i = 0; i < variable_declarations.size(); ++i) {
        function << variable_declarations[i].getCode() << ";";
    }
    function << std::endl
             << "/* statements section */" << std::endl;
    for (size_t i = 0; i < statements.size(); ++i) {
        function << statements[i]->getCode()->code_ << ";";
    }
    function << "}";

    return FunctionDeclaration(function.str());
}

FunctionBuilder& FunctionBuilder::returns(GeneratableDataTypePtr type) {
    returnType = type;
    return *this;
}

FunctionBuilder& FunctionBuilder::addParameter(VariableDeclaration var_decl) {
    parameters.push_back(var_decl);
    return *this;
}
FunctionBuilder& FunctionBuilder::addStatement(StatementPtr statement) {
    if (statement)
        statements.push_back(statement);
    return *this;
}

FunctionBuilder& FunctionBuilder::addVariableDeclaration(VariableDeclaration vardecl) {
    variable_declarations.push_back(vardecl);
    return *this;
}

}// namespace NES
