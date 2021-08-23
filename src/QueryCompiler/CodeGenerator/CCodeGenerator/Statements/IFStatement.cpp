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
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation {

IFStatement::IFStatement(const Statement& condExpr)
    : conditionalExpression(condExpr.createCopy()), trueCaseStatement(std::make_shared<CompoundStatement>()) {}
IFStatement::IFStatement(const Statement& condExpr, const Statement& condTrueStmt)
    : conditionalExpression(condExpr.createCopy()), trueCaseStatement(std::make_shared<CompoundStatement>()) {
    trueCaseStatement->addStatement(condTrueStmt.createCopy());
}

IFStatement::IFStatement(StatementPtr condExpr, const StatementPtr& condTrueStmt)
    : conditionalExpression(std::move(condExpr)), trueCaseStatement(std::make_shared<CompoundStatement>()) {
    trueCaseStatement->addStatement(condTrueStmt);
}

StatementType IFStatement::getStamentType() const { return IF_STMT; }
CodeExpressionPtr IFStatement::getCode() const {
    std::stringstream code;
    code << "if(" << conditionalExpression->getCode()->code_ << "){" << std::endl;
    code << trueCaseStatement->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}
StatementPtr IFStatement::createCopy() const { return std::make_shared<IFStatement>(*this); }

CompoundStatementPtr IFStatement::getCompoundStatement() { return trueCaseStatement; }
}// namespace NES::QueryCompilation
