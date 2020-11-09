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

#include <memory>
#include <string>

#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>

namespace NES {

const std::string toString(const BinaryOperatorType& type) {
    const char* const names[] = {"EQUAL_OP",
                                 "UNEQUAL_OP",
                                 "LESS_THEN_OP",
                                 "LESS_THEN_EQUAL_OP",
                                 "GREATER_THEN_OP",
                                 "GREATER_THEN_EQUAL_OP",
                                 "PLUS_OP",
                                 "MINUS_OP",
                                 "MULTIPLY_OP",
                                 "DIVISION_OP",
                                 "MODULO_OP",
                                 "LOGICAL_AND_OP",
                                 "LOGICAL_OR_OP",
                                 "BITWISE_AND_OP",
                                 "BITWISE_OR_OP",
                                 "BITWISE_XOR_OP",
                                 "BITWISE_LEFT_SHIFT_OP",
                                 "BITWISE_RIGHT_SHIFT_OP",
                                 "ASSIGNMENT_OP",
                                 "ARRAY_REFERENCE_OP",
                                 "MEMBER_SELECT_POINTER_OP",
                                 "MEMBER_SELECT_REFERENCE_OP"};
    return std::string(names[type]);
}

const CodeExpressionPtr toCodeExpression(const BinaryOperatorType& type) {
    const char* const names[] = {
        "==",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "+",
        "-",
        "*",
        "/",
        "%",
        "&&",
        "||",
        "&",
        "|",
        "^",
        "<<",
        ">>",
        "=",
        "[]",
        "->",
        ".",
    };
    return std::make_shared<CodeExpression>(names[type]);
}

BinaryOperatorStatement::BinaryOperatorStatement(const ExpressionStatment& lhs, const BinaryOperatorType& op,
                                                 const ExpressionStatment& rhs, BracketMode bracket_mode)
    : lhs_(lhs.copy()), rhs_(rhs.copy()), op_(op), bracket_mode_(bracket_mode) {
}

BinaryOperatorStatement BinaryOperatorStatement::addRight(const BinaryOperatorType& op, const VarRefStatement& rhs,
                                                          BracketMode bracket_mode) {
    return BinaryOperatorStatement(*this, op, rhs, bracket_mode);
}

StatementPtr BinaryOperatorStatement::assignToVariable(const VarRefStatement&) { return StatementPtr(); }

StatementType BinaryOperatorStatement::getStamentType() const { return BINARY_OP_STMT; }

const CodeExpressionPtr BinaryOperatorStatement::getCode() const {
    CodeExpressionPtr code;
    if (ARRAY_REFERENCE_OP == op_) {
        code = combine(lhs_->getCode(), std::make_shared<CodeExpression>("["));
        code = combine(code, rhs_->getCode());
        code = combine(code, std::make_shared<CodeExpression>("]"));
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

const ExpressionStatmentPtr BinaryOperatorStatement::copy() const {
    return std::make_shared<BinaryOperatorStatement>(*this);
}

BinaryOperatorStatement::~BinaryOperatorStatement() {}

/** \brief small utility operator overloads to make code generation simpler and */

BinaryOperatorStatement assign(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, ASSIGNMENT_OP, rhs);
}

// BinaryOperatorStatement ExpressionStatment::operator =(const ExpressionStatment &ref){
// return BinaryOperatorStatement(*this, ASSIGNMENT_OP, ref);
//}

//  BinaryOperatorStatement ExpressionStatment::operator [](const ExpressionStatment &ref){
//    return BinaryOperatorStatement(*this, ARRAY_REFERENCE_OP, ref);
//  }

//  BinaryOperatorStatement ExpressionStatment::accessPtr(const ExpressionStatment &ref){
//     return BinaryOperatorStatement(*this, MEMBER_SELECT_POINTER_OP, ref);
//  }

//  BinaryOperatorStatement ExpressionStatment::accessRef(const ExpressionStatment &ref){
//     return BinaryOperatorStatement(*this, MEMBER_SELECT_REFERENCE_OP, ref);
//  }

//  BinaryOperatorStatement ExpressionStatment::assign(const ExpressionStatment &ref){
//   return BinaryOperatorStatement(*this, ASSIGNMENT_OP, ref);
//  }

BinaryOperatorStatement operator==(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, EQUAL_OP, rhs);
}
BinaryOperatorStatement operator!=(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, UNEQUAL_OP, rhs);
}
BinaryOperatorStatement operator<(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, LESS_THAN_OP, rhs);
}
BinaryOperatorStatement operator<=(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, LESS_THAN_EQUAL_OP, rhs);
}
BinaryOperatorStatement operator>(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, GREATER_THAN_OP, rhs);
}
BinaryOperatorStatement operator>=(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, GREATER_THAN_EQUAL_OP, rhs);
}
BinaryOperatorStatement operator+(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, PLUS_OP, rhs);
}
BinaryOperatorStatement operator-(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, MINUS_OP, rhs);
}
BinaryOperatorStatement operator*(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, MULTIPLY_OP, rhs);
}
BinaryOperatorStatement operator/(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, DIVISION_OP, rhs);
}
BinaryOperatorStatement operator%(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, MODULO_OP, rhs);
}
BinaryOperatorStatement operator&&(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, LOGICAL_AND_OP, rhs);
}
BinaryOperatorStatement operator||(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, LOGICAL_OR_OP, rhs);
}
BinaryOperatorStatement operator&(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, BITWISE_AND_OP, rhs);
}
BinaryOperatorStatement operator|(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, BITWISE_OR_OP, rhs);
}
BinaryOperatorStatement operator^(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, BITWISE_XOR_OP, rhs);
}
BinaryOperatorStatement operator<<(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, BITWISE_LEFT_SHIFT_OP, rhs);
}
BinaryOperatorStatement operator>>(const ExpressionStatment& lhs, const ExpressionStatment& rhs) {
    return BinaryOperatorStatement(lhs, BITWISE_RIGHT_SHIFT_OP, rhs);
}

}// namespace NES
