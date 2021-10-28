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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_FUNCTION_CALL_STATEMENT_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_FUNCTION_CALL_STATEMENT_HPP_
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ExpressionStatement.hpp>

#include <memory>
#include <vector>

namespace NES::QueryCompilation {
class FunctionCallStatement : public ExpressionStatement {
  public:
    explicit inline FunctionCallStatement(std::string const& f) noexcept : functionName(f){};
    explicit inline FunctionCallStatement(std::string&& f) noexcept : functionName(std::move(f)){};

    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    template<typename T, typename = std::enable_if_t<std::is_constructible_v<std::string, T>>>
    static inline FunctionCallStatementPtr create(T&& f) noexcept {
        return std::make_shared<FunctionCallStatement>(std::forward<T>(f));
    }

    [[nodiscard]] ExpressionStatementPtr copy() const override;

    virtual void addParameter(const ExpressionStatement& expr);
    virtual void addParameter(ExpressionStatementPtr expr);

    ~FunctionCallStatement() override = default;

  private:
    std::string functionName;
    std::vector<ExpressionStatementPtr> expressions;
};

}// namespace NES::QueryCompilation

#endif// NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_STATEMENTS_FUNCTION_CALL_STATEMENT_HPP_
