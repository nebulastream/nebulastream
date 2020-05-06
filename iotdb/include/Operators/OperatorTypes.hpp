//
// Created by pgrulich on 17.07.19.
//

#ifndef OPERATORTYPES_HPP
#define OPERATORTYPES_HPP

namespace NES {

enum UnaryOperatorType {
    ADDRESS_OF_OP,
    DEREFERENCE_POINTER_OP,
    PREFIX_INCREMENT_OP,
    PREFIX_DECREMENT_OP,
    POSTFIX_INCREMENT_OP,
    POSTFIX_DECREMENT_OP,
    BITWISE_COMPLEMENT_OP,
    LOGICAL_NOT_OP,
    SIZE_OF_TYPE_OP
};

const std::string toString(const UnaryOperatorType& type);

enum BinaryOperatorType {
    EQUAL_OP,
    UNEQUAL_OP,
    LESS_THEN_OP,
    LESS_THEN_EQUAL_OP,
    GREATER_THEN_OP,
    GREATER_THEN_EQUAL_OP,
    PLUS_OP,
    MINUS_OP,
    MULTIPLY_OP,
    DIVISION_OP,
    MODULO_OP,
    LOGICAL_AND_OP,
    LOGICAL_OR_OP,
    BITWISE_AND_OP,
    BITWISE_OR_OP,
    BITWISE_XOR_OP,
    BITWISE_LEFT_SHIFT_OP,
    BITWISE_RIGHT_SHIFT_OP,
    ASSIGNMENT_OP,
    ARRAY_REFERENCE_OP,
    MEMBER_SELECT_POINTER_OP,
    MEMBER_SELECT_REFERENCE_OP
};

const std::string toString(const BinaryOperatorType& type);
}// namespace NES

#endif//OPERATORTYPES_HPP
