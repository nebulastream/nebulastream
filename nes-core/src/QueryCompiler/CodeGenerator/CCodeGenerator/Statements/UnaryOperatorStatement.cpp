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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <Util/Logger/Logger.hpp>
#include <array>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::QueryCompilation {
std::string toString(const UnaryOperatorType& type) {
    constexpr std::array<char const*, 17> names{"ABS_VALUE_OF_OP",
                                                "ADDRESS_OF_OP",
                                                "BITWISE_COMPLEMENT_OP",
                                                "CEIL_OP",
                                                "DEREFERENCE_POINTER_OP",
                                                "EXP_OP",
                                                "FLOOR_OP",
                                                "LOG_OP",
                                                "LOG10_OP",
                                                "LOGICAL_NOT_OP",
                                                "PREFIX_INCREMENT_OP",
                                                "PREFIX_DECREMENT_OP",
                                                "POSTFIX_INCREMENT_OP",
                                                "POSTFIX_DECREMENT_OP",
                                                "ROUND_OP",
                                                "SIZE_OF_TYPE_OP",
                                                "SQRT_OP"};
    return names[magic_enum::enum_integer(type)];
}

CodeExpressionPtr toCodeExpression(const UnaryOperatorType& type) {
    const char* const names[] =
        {"abs", "&", "~", "ceil", "*", "exp", "floor", "log", "log10", "!", "++", "--", "++", "--", "round", "sizeof", "sqrt"};
    return std::make_shared<CodeExpression>(names[magic_enum::enum_integer(type)]);
}

UnaryOperatorStatement::UnaryOperatorStatement(const ExpressionStatement& expr,
                                               const UnaryOperatorType& op,
                                               BracketMode bracket_mode)
    : expr_(expr.copy()), op(op), bracket_mode(bracket_mode) {
    NES_ASSERT(op != UnaryOperatorType::INVALID_UNARY_OPERATOR_TYPE, "invalid operator");
}

StatementType UnaryOperatorStatement::getStamentType() const { return StatementType::UNARY_OP_STMT; }
CodeExpressionPtr UnaryOperatorStatement::getCode() const {
    CodeExpressionPtr code;
    if (UnaryOperatorType::POSTFIX_INCREMENT_OP == op || UnaryOperatorType::POSTFIX_DECREMENT_OP == op) {
        /* postfix operators */
        code = combine(expr_->getCode(), toCodeExpression(op));
    } else if (UnaryOperatorType::ABSOLUTE_VALUE_OF_OP == op || UnaryOperatorType::ROUND_OP == op ||
               UnaryOperatorType::CEIL_OP == op || UnaryOperatorType::EXP_OP == op || UnaryOperatorType::FLOOR_OP == op ||
               UnaryOperatorType::LOG_OP == op || UnaryOperatorType::LOG10_OP == op ||
               UnaryOperatorType::SIZE_OF_TYPE_OP == op || UnaryOperatorType::SQRT_OP == op) {
        code = combine(toCodeExpression(op), std::make_shared<CodeExpression>("("));
        code = combine(code, expr_->getCode());
        code = combine(code, std::make_shared<CodeExpression>(")"));
    } else {
        /* prefix operators */
        code = combine(toCodeExpression(op), expr_->getCode());
    }
    std::string ret;
    if (bracket_mode == BracketMode::BRACKETS) {
        ret = std::string("(") + code->code_ + std::string(")");
    } else {
        ret = code->code_;
    }
    return std::make_shared<CodeExpression>(ret);
}

ExpressionStatementPtr UnaryOperatorStatement::copy() const { return std::make_shared<UnaryOperatorStatement>(*this); }

UnaryOperatorStatement operator&(const ExpressionStatement& ref) { return UnaryOperatorStatement(ref, UnaryOperatorType::ADDRESS_OF_OP); }
UnaryOperatorStatement operator*(const ExpressionStatement& ref) { return UnaryOperatorStatement(ref, UnaryOperatorType::DEREFERENCE_POINTER_OP); }
UnaryOperatorStatement operator++(const ExpressionStatement& ref) { return UnaryOperatorStatement(ref, UnaryOperatorType::PREFIX_INCREMENT_OP); }
UnaryOperatorStatement operator--(const ExpressionStatement& ref) { return UnaryOperatorStatement(ref, UnaryOperatorType::PREFIX_DECREMENT_OP); }
UnaryOperatorStatement operator~(const ExpressionStatement& ref) { return UnaryOperatorStatement(ref, UnaryOperatorType::BITWISE_COMPLEMENT_OP); }
UnaryOperatorStatement operator!(const ExpressionStatement& ref) { return UnaryOperatorStatement(ref, UnaryOperatorType::LOGICAL_NOT_OP); }

UnaryOperatorStatement sizeOf(const ExpressionStatement& ref) { return UnaryOperatorStatement(ref, UnaryOperatorType::SIZE_OF_TYPE_OP); }
}// namespace NES::QueryCompilation
