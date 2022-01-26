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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_PREDICATEDFILTERSTATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_PREDICATEDFILTERSTATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/VarRefStatement.hpp>

namespace NES {
namespace QueryCompilation {
class PredicatedFilterStatement : public Statement {
  public:
    static ExpressionStatementPtr generatePredicateEvaluationCode(const ExpressionStatement& condExpr,
                                                                  const VariableDeclaration& tuplePassesFilter,
                                                                  bool tuplePassesPredicateIsDeclared);

    explicit PredicatedFilterStatement(const ExpressionStatement& condExpr,
                                       const VariableDeclaration& tuplePassesFilter,
                                       bool tuplePassesPredicateIsDeclared);

    PredicatedFilterStatement(const ExpressionStatement& condExpr,
                              const VariableDeclaration& tuplePassesFilter,
                              bool tuplePassesPredicateIsDeclared,
                              const Statement& predicatedCode);
    PredicatedFilterStatement(const ExpressionStatementPtr condExpr,
                              const VariableDeclarationPtr tuplePassesFilter,
                              bool tuplePassesPredicateIsDeclared,
                              const StatementPtr predicatedCode);

    [[nodiscard]] StatementType getStamentType() const override;
    [[nodiscard]] CodeExpressionPtr getCode() const override;
    [[nodiscard]] StatementPtr createCopy() const override;

    CompoundStatementPtr getCompoundStatement();

    ~PredicatedFilterStatement() noexcept override = default;

  private:
    ExpressionStatementPtr predicateEvaluation;
    CompoundStatementPtr predicatedCode;
};

using PredicatedFilter = PredicatedFilterStatement;
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_PREDICATEDFILTERSTATEMENT_HPP_
