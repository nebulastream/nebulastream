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

#include <memory>
#include <string>

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/MultiOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>

namespace NES::QueryCompilation {
std::string toString(const MultiOperatorType& type) {
    const char* const names[] = {"CREATETENSOR_OP"};
    return std::string(names[type]);
}

CodeExpressionPtr toCodeExpression(const MultiOperatorType& type) {
    //TODO: figure out what to put here
    constexpr char const* const names[] = {
        "createTensor",
    };
    return std::make_shared<CodeExpression>(names[type]);
}

MultiOperatorStatement::MultiOperatorStatement(const std::vector<ExpressionStatement&> children,
                                               const MultiOperatorType& op,
                                               BracketMode bracket_mode)
    : children_(children), op_(op), bracket_mode_(bracket_mode) {
}//todo: check if this works because it should actually be copied with the copy constructor and create a new pointer

MultiOperatorStatement::MultiOperatorStatement(const std::vector<ExpressionStatementPtr&> children,
                                               const BinaryOperatorType& op,
                                               BracketMode bracket_mode)
    : children(children), op_(op), bracket_mode_(bracket_mode) {}

//todo: what is this, what does this do?
MultiOperatorStatement
MultiOperatorStatement::addRight(const BinaryOperatorType& op, const VarRefStatement& rhs, BracketMode bracket_mode) {
    return MultiOperatorStatement(*this, op, rhs, bracket_mode);
}

StatementPtr MultiOperatorStatement::assignToVariable(const VarRefStatement&) { return StatementPtr(); }

StatementType MultiOperatorStatement::getStamentType() const { return MULTI_OP_STMT; }

//todo: work on main portion!!!
CodeExpressionPtr MultiOperatorStatement::getCode() const {
    CodeExpressionPtr code;
    if (ARRAY_REFERENCE_OP == op_) {
        code = combine(lhs_->getCode(), std::make_shared<CodeExpression>("["));
        code = combine(code, rhs_->getCode());
        code = combine(code, std::make_shared<CodeExpression>("]"));
    } else if (POWER_OP == op_ || MODULO_OP == op_) {
        code = combine(toCodeExpression(op_), std::make_shared<CodeExpression>("("));// -> "pow(" or "fmod("
        code = combine(code, lhs_->getCode());
        code = combine(code, std::make_shared<CodeExpression>(","));
        code = combine(code, rhs_->getCode());
        code = combine(code, std::make_shared<CodeExpression>(")"));
    } else {
        code = combine(lhs_->getCode(), toCodeExpression(op_));
        code = combine(code, rhs_->getCode());
    }

    std::string ret;
    if (bracket_mode_ == BRACKETS) {
        ret = std::string("(") + code->code_ + std::string(")");
    } else {
        ret = code->code_;
    }
    return std::make_shared<CodeExpression>(ret);
}

ExpressionStatementPtr MultiOperatorStatement::copy() const { return std::make_shared<MultiOperatorStatement>(*this); }

/** \brief small utility operator overloads to make code generation simpler and */

MultiOperatorStatement assign(const ExpressionStatement& lhs, const ExpressionStatement& rhs) {
    return MultiOperatorStatement(lhs, ASSIGNMENT_OP, rhs);
}

}// namespace NES::QueryCompilation
