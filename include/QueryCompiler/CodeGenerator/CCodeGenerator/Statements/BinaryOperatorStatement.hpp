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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_BINARY_OPERATOR_STATEMENT_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_BINARY_OPERATOR_STATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>

namespace NES::QueryCompilation {
CodeExpressionPtr toCodeExpression(const BinaryOperatorType& type);

class BinaryOperatorStatement : public ExpressionStatement {
  public:
    BinaryOperatorStatement(const ExpressionStatementPtr& lhs,
                            const BinaryOperatorType& op,
                            const ExpressionStatementPtr& rhs,
                            BracketMode bracket_mode = NO_BRACKETS);

    BinaryOperatorStatement(const ExpressionStatement& lhs,
                            const BinaryOperatorType& op,
                            const ExpressionStatement& rhs,
                            BracketMode bracket_mode = NO_BRACKETS);

    ~BinaryOperatorStatement() override = default;

    BinaryOperatorStatement
    addRight(const BinaryOperatorType& op, const VarRefStatement& rhs, BracketMode bracket_mode = NO_BRACKETS);

    static StatementPtr assignToVariable(const VarRefStatement& lhs);

    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    [[nodiscard]] ExpressionStatementPtr copy() const override;

  private:
    ExpressionStatementPtr lhs_;
    ExpressionStatementPtr rhs_;
    BinaryOperatorType op_;
    BracketMode bracket_mode_;
};

/** \brief small utility operator overloads to make code generation simpler and */

BinaryOperatorStatement assign(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator==(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator!=(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator<(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator<=(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator>(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator>=(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator+(const ExpressionStatement&, const ExpressionStatement& rhs);

BinaryOperatorStatement operator-(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator*(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator/(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator%(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator&&(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator||(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator&(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator|(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator^(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator<<(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

BinaryOperatorStatement operator>>(const ExpressionStatement& lhs, const ExpressionStatement& rhs);

}// namespace NES::QueryCompilation
#endif  // NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_BINARY_OPERATOR_STATEMENT_HPP_
