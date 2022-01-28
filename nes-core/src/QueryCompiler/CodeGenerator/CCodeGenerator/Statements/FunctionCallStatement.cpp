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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
namespace NES::QueryCompilation {

StatementType FunctionCallStatement::getStamentType() const { return FUNC_CALL_STMT; }

CodeExpressionPtr FunctionCallStatement::getCode() const {

    u_int32_t i = 0;

    CodeExpressionPtr code;
    code = combine(std::make_shared<CodeExpression>(functionName), std::make_shared<CodeExpression>("("));
    for (i = 0; i < expressions.size(); i++) {
        if (i != 0) {
            code = combine(code, std::make_shared<CodeExpression>(", "));
        }
        code = combine(code, expressions.at(i)->getCode());
    }
    code = combine(code, std::make_shared<CodeExpression>(")"));
    return code;
}

ExpressionStatementPtr FunctionCallStatement::copy() const { return std::make_shared<FunctionCallStatement>(*this); }

void FunctionCallStatement::addParameter(const ExpressionStatement& expr) { expressions.push_back(expr.copy()); }

void FunctionCallStatement::addParameter(ExpressionStatementPtr expr) { expressions.push_back(expr); }

}// namespace NES::QueryCompilation
