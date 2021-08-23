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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/PredicatedFilterStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation {

PredicatedFilterStatement::PredicatedFilterStatement(const Statement& condExpr, const VariableDeclaration& indexVariable)
: conditionalExpression(condExpr.createCopy()), indexVariable(indexVariable), predicatedCode(std::make_shared<CompoundStatement>()) {}

PredicatedFilterStatement::PredicatedFilterStatement(const Statement& condExpr, const VariableDeclaration& indexVariable, const Statement& predicatedCode)
: conditionalExpression(condExpr.createCopy()), indexVariable(indexVariable), predicatedCode(std::make_shared<CompoundStatement>()) {
    this->predicatedCode->addStatement(predicatedCode.createCopy());
}

PredicatedFilterStatement::PredicatedFilterStatement(StatementPtr condExpr, const VariableDeclarationPtr indexVariable, const StatementPtr predicatedCode)
: conditionalExpression(std::move(condExpr)), indexVariable(indexVariable), predicatedCode(std::make_shared<CompoundStatement>()) {
    this->predicatedCode->addStatement(predicatedCode);
}

StatementType PredicatedFilterStatement::getStamentType() const { return IF_STMT; }
CodeExpressionPtr PredicatedFilterStatement::getCode() const {
    std::stringstream code;
    // execute lower code statements as if they were not predicated
    code << predicatedCode->getCode()->code_ << std::endl;
    // but then, only increase the index of the result buffer write location if condition is fulfilled
    code << indexVariable.getCode()->code_  << " += " <<  conditionalExpression->getCode()->code_ << std::endl;
    // thus only tuples that pass the condition will persist in the result buffer
    return std::make_shared<CodeExpression>(code.str());
}
StatementPtr PredicatedFilterStatement::createCopy() const { return std::make_shared<PredicatedFilterStatement>(*this); }

CompoundStatementPtr PredicatedFilterStatement::getCompoundStatement() { return predicatedCode; }
}// namespace NES::QueryCompilation
