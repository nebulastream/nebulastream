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

#include <QueryCompiler/CCodeGenerator/Statements/VarDeclStatement.hpp>

namespace NES {

StatementType VarDeclStatement::getStamentType() const { return VAR_DEC_STMT; }

const CodeExpressionPtr VarDeclStatement::getCode() const {
    return std::make_shared<CodeExpression>(variableDeclaration->getCode());
}

const ExpressionStatmentPtr VarDeclStatement::copy() const { return std::make_shared<VarDeclStatement>(*this); }

VarDeclStatement::VarDeclStatement(const VariableDeclaration& var_decl)
    : variableDeclaration(std::dynamic_pointer_cast<VariableDeclaration>(var_decl.copy())) {}

VarDeclStatement::~VarDeclStatement() {}

}// namespace NES
