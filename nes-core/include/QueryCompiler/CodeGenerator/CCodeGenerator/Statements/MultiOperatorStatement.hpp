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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_MULTIOPERATORSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_MULTIOPERATORSTATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/CodeGenerator/OperatorTypes.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES::QueryCompilation {
CodeExpressionPtr toCodeExpression(const BinaryOperatorType& type);

class MultiOperatorStatement : public ExpressionStatement {
  public:
    MultiOperatorStatement(const std::vector<ExpressionStatementPtr&> children,
                           const MultiOperatorType& op,
                           BracketMode bracket_mode = NO_BRACKETS);

    MultiOperatorStatement(const std::vector<ExpressionStatement&> children,
                           const MultiOperatorType& op,
                           BracketMode bracket_mode = NO_BRACKETS);

    ~MultiOperatorStatement() override = default;

    MultiOperatorStatement
    addRight(const BinaryOperatorType& op, const VarRefStatement& rhs, BracketMode bracket_mode = NO_BRACKETS);

    static StatementPtr assignToVariable(const VarRefStatement& lhs);

    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    [[nodiscard]] ExpressionStatementPtr copy() const override;

  private:
    std::vector<ExpressionStatementPtr> children_{};
    BinaryOperatorType op_;
    BracketMode bracket_mode_;
};

#endif//NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_MULTIOPERATORSTATEMENT_HPP_
}