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

#ifndef NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_STATEMENT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_STATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/VariableDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <memory>
#include <string>
#include <utility>

namespace NES {
namespace QueryCompilation {
enum StatementType {
    RETURN_STMT,
    CONTINUE_STMT,
    IF_STMT,
    IF_ELSE_STMT,
    PREDICATED_FILTER_STMT,
    FOR_LOOP_STMT,
    FUNC_CALL_STMT,
    VAR_REF_STMT,
    VAR_DEC_STMT,
    CONSTANT_VALUE_EXPR_STMT,
    TYPE_CAST_EXPR_STMT,
    BINARY_OP_STMT,
    UNARY_OP_STMT,
    COMPOUND_STMT,
    STD_OUT_STMT,
    COMMENT_STMT
};

enum BracketMode { NO_BRACKETS, BRACKETS };

class Statement;
using StatementPtr = std::shared_ptr<Statement>;

class Statement {
  public:
    Statement() = default;

    Statement(const Statement&) = default;

    /**
     * @brief method to get the statement type
     * @return StatementType
     */
    [[nodiscard]] virtual StatementType getStamentType() const = 0;

    /**
     * @brief method to get the code as code expressions
     * @return CodeExpressionPtr
     */
    [[nodiscard]] virtual CodeExpressionPtr getCode() const = 0;

    /**
     * @brief method to create a copy of this statement
     * @return copy of statement
     */
    [[nodiscard]] virtual StatementPtr createCopy() const = 0;

    /** @brief virtual copy constructor */
    virtual ~Statement() = default;
};

class AssignmentStatement {
  public:
    VariableDeclaration lhs_tuple_var;
    VariableDeclaration lhs_field_var;
    VariableDeclaration lhs_index_var;
    VariableDeclaration rhs_tuple_var;
    VariableDeclaration rhs_field_var;
    VariableDeclaration rhs_index_var;
};
}// namespace QueryCompilation
}// namespace NES
#endif  // NES_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_STATEMENT_HPP_
