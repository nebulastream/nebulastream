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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_VAR_DECL_STATEMENT_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_VAR_DECL_STATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ExpressionStatement.hpp>

namespace NES {
namespace QueryCompilation {
class VarDeclStatement : public ExpressionStatement {
  public:
    explicit VarDeclStatement(const VariableDeclaration& var_decl);

    ~VarDeclStatement() noexcept override = default;

    [[nodiscard]] StatementType getStamentType() const override { return VAR_DEC_STMT; }

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    [[nodiscard]] ExpressionStatementPtr copy() const override;

  private:
    std::shared_ptr<VariableDeclaration> variableDeclaration;
};

using VarDecl = VarDeclStatement;
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_VAR_DECL_STATEMENT_HPP_
