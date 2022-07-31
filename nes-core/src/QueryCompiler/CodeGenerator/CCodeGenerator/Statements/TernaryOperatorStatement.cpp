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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/TernaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <sstream>
#include <utility>

namespace NES::QueryCompilation {

TernaryOperatorStatement::TernaryOperatorStatement(const ExpressionStatement& condExpr,
                                                   const ExpressionStatement& condTrueExpr,
                                                   const ExpressionStatement& condFalseExpr)
    : conditionalExpression(condExpr.copy()), trueCaseExpression(condTrueExpr.copy()), falseCaseExpression(condFalseExpr.copy()) {
}

TernaryOperatorStatement::TernaryOperatorStatement(ExpressionStatementPtr& condExpr,
                                                   const ExpressionStatementPtr& condTrueExpr,
                                                   const ExpressionStatementPtr& condFalseExpr)
    : conditionalExpression(condExpr), trueCaseExpression(condTrueExpr), falseCaseExpression(condFalseExpr) {}

StatementType TernaryOperatorStatement::getStamentType() const { return TERNARY_OP_STMT; }

CodeExpressionPtr TernaryOperatorStatement::getCode() const {
    std::stringstream code;
    code << conditionalExpression->getCode()->code_ << "?" << trueCaseExpression->getCode()->code_ << ":"
         << falseCaseExpression->getCode()->code_;
    return std::make_shared<CodeExpression>(code.str());
}
ExpressionStatementPtr TernaryOperatorStatement::copy() const { return std::make_shared<TernaryOperatorStatement>(*this); }
}// namespace NES::QueryCompilation
