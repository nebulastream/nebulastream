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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_FORLOOPSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_FORLOOPSTATEMENT_HPP_
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <vector>
namespace NES {
namespace QueryCompilation {
class ForLoopStatement : public Statement {
  public:
    ForLoopStatement(DeclarationPtr varDeclaration,
                     ExpressionStatementPtr condition,
                     ExpressionStatementPtr advance,
                     std::vector<StatementPtr> loop_body = std::vector<StatementPtr>());

    ~ForLoopStatement() noexcept override = default;

    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    [[nodiscard]] StatementPtr createCopy() const override;

    [[nodiscard]] CompoundStatementPtr getCompoundStatement();

    void addStatement(const StatementPtr& stmt);

  private:
    DeclarationPtr varDeclaration;
    ExpressionStatementPtr condition;
    ExpressionStatementPtr advance;
    CompoundStatementPtr body;
};

using FOR = ForLoopStatement;
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_FORLOOPSTATEMENT_HPP_
