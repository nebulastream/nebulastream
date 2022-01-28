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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_UNARYOPERATORSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_UNARYOPERATORSTATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>

namespace NES::QueryCompilation {
CodeExpressionPtr toCodeExpression(const UnaryOperatorType& type);

class UnaryOperatorStatement : public ExpressionStatement {
  public:
    UnaryOperatorStatement(const ExpressionStatement& expr, const UnaryOperatorType& op, BracketMode bracket_mode = NO_BRACKETS);

    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    [[nodiscard]] ExpressionStatementPtr copy() const override;

    ~UnaryOperatorStatement() override = default;

  private:
    ExpressionStatementPtr expr_{nullptr};
    UnaryOperatorType op{INVALID_UNARY_OPERATOR_TYPE};
    BracketMode bracket_mode{NO_BRACKETS};
};

UnaryOperatorStatement operator&(const ExpressionStatement& ref);

UnaryOperatorStatement operator*(const ExpressionStatement& ref);

UnaryOperatorStatement operator++(const ExpressionStatement& ref);

UnaryOperatorStatement operator--(const ExpressionStatement& ref);

UnaryOperatorStatement operator~(const ExpressionStatement& ref);

UnaryOperatorStatement operator!(const ExpressionStatement& ref);

UnaryOperatorStatement sizeOf(const ExpressionStatement& ref);

}// namespace NES::QueryCompilation
#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_UNARYOPERATORSTATEMENT_HPP_
