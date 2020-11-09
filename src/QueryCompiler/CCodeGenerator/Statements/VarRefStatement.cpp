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


#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>

namespace NES {

StatementType VarRefStatement::getStamentType() const { return VAR_REF_STMT; }

const CodeExpressionPtr VarRefStatement::getCode() const { return varDeclaration->getIdentifier(); }

const ExpressionStatmentPtr VarRefStatement::copy() const { return std::make_shared<VarRefStatement>(*this); }

VarRefStatement::VarRefStatement(const VariableDeclaration& var_decl)
    : varDeclaration(std::dynamic_pointer_cast<VariableDeclaration>(var_decl.copy())) {
}

VarRefStatement::VarRefStatement(VariableDeclarationPtr varDeclaration)
    : varDeclaration(std::move(varDeclaration)) {
}

VarRefStatement::~VarRefStatement() {}

}// namespace NES
