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
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/IFElseStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <sstream>

namespace NES::QueryCompilation {
IfElseStatement::IfElseStatement(const Statement& cond_expr, const Statement& cond_true_stmt, const Statement& cond_false_stmt)
    : conditionalExpression(cond_expr.createCopy()), trueCaseStatement(new CompoundStatement()),
      falseCaseStatement(new CompoundStatement()) {
    trueCaseStatement->addStatement(cond_true_stmt.createCopy());
    falseCaseStatement->addStatement(cond_false_stmt.createCopy());
}

StatementType IfElseStatement::getStamentType() const { return IF_ELSE_STMT; }
CodeExpressionPtr IfElseStatement::getCode() const {
    std::stringstream code;
    code << "if(" << conditionalExpression->getCode()->code_ << "){" << std::endl;
    code << trueCaseStatement->getCode()->code_ << std::endl;
    code << "} else {" << std::endl;
    code << falseCaseStatement->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}

StatementPtr IfElseStatement::createCopy() const { return std::make_shared<IfElseStatement>(*this); }

CompoundStatementPtr IfElseStatement::getTrueCaseCompoundStatement() { return trueCaseStatement; }
CompoundStatementPtr IfElseStatement::getFalseCaseCompoundStatement() { return falseCaseStatement; }
}// namespace NES::QueryCompilation
