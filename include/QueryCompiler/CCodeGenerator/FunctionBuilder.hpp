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

#pragma once

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class GeneratableDataType;
typedef std::shared_ptr<GeneratableDataType> GeneratableDataTypePtr;

class FunctionBuilder {
  private:
    std::string name;
    GeneratableDataTypePtr returnType;
    std::vector<VariableDeclaration> parameters;
    std::vector<VariableDeclaration> variable_declarations;
    std::vector<StatementPtr> statements;
    FunctionBuilder(const std::string& function_name);

  public:
    static FunctionBuilder create(const std::string& function_name);

    FunctionBuilder& returns(GeneratableDataTypePtr returnType_);
    FunctionBuilder& addParameter(VariableDeclaration var_decl);
    FunctionBuilder& addStatement(StatementPtr statement);
    FunctionBuilder& addVariableDeclaration(VariableDeclaration var_decl);
    FunctionDeclaration build();
};

}// namespace NES
