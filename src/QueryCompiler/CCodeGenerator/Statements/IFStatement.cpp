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

#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <sstream>

namespace NES {

IFStatement::~IFStatement() {}

IFStatement::IFStatement(const Statement& cond_expr)
    : conditionalExpression(cond_expr.createCopy()), trueCaseStatement(new CompoundStatement()) {}
IFStatement::IFStatement(const Statement& cond_expr, const Statement& cond_true_stmt)
    : conditionalExpression(cond_expr.createCopy()), trueCaseStatement(new CompoundStatement()) {
    trueCaseStatement->addStatement(cond_true_stmt.createCopy());
}

IFStatement::IFStatement(const StatementPtr& cond_expr, const StatementPtr& cond_true_stmt)
    : conditionalExpression(cond_expr), trueCaseStatement(new CompoundStatement()) {
    trueCaseStatement->addStatement(cond_true_stmt);
}

StatementType IFStatement::getStamentType() const { return IF_STMT; }
const CodeExpressionPtr IFStatement::getCode() const {
    std::stringstream code;
    code << "if(" << conditionalExpression->getCode()->code_ << "){" << std::endl;
    code << trueCaseStatement->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}
const StatementPtr IFStatement::createCopy() const { return std::make_shared<IFStatement>(*this); }

const CompoundStatementPtr IFStatement::getCompoundStatement() { return trueCaseStatement; }

}// namespace NES
