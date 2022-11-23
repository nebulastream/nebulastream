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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_TERNARYOPERATORSTATEMENT_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_TERNARYOPERATORSTATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>

namespace NES {
namespace QueryCompilation {

class TernaryOperatorStatement : public ExpressionStatement {
  public:
    TernaryOperatorStatement(const ExpressionStatement& condExpr,
                             const ExpressionStatement& condTrueExpr,
                             const ExpressionStatement& condFalseExpr);
    TernaryOperatorStatement(ExpressionStatementPtr& condExpr,
                             const ExpressionStatementPtr& condTrueExpr,
                             const ExpressionStatementPtr& condFalseExpr);

    ~TernaryOperatorStatement() override = default;

    [[nodiscard]] StatementType getStamentType() const override;
    [[nodiscard]] CodeExpressionPtr getCode() const override;
    [[nodiscard]] ExpressionStatementPtr copy() const override;

  private:
    const ExpressionStatementPtr conditionalExpression;
    const ExpressionStatementPtr trueCaseExpression;
    const ExpressionStatementPtr falseCaseExpression;
};
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_TERNARYOPERATORSTATEMENT_HPP_
