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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_OPERATOR_TYPES_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_OPERATOR_TYPES_HPP_

namespace NES {

enum UnaryOperatorType {
    ABSOLUTE_VALUE_OF_OP,
    ADDRESS_OF_OP,
    BITWISE_COMPLEMENT_OP,
    CEIL_OP,
    DEREFERENCE_POINTER_OP,
    EXP_OP,
    FLOOR_OP,
    LOG_OP,
    LOG10_OP,
    LOGICAL_NOT_OP,
    PREFIX_INCREMENT_OP,
    PREFIX_DECREMENT_OP,
    POSTFIX_INCREMENT_OP,
    POSTFIX_DECREMENT_OP,
    ROUND_OP,
    SIZE_OF_TYPE_OP,
    SQRT_OP,
    INVALID_UNARY_OPERATOR_TYPE
};

const std::string toString(const UnaryOperatorType& type);

enum BinaryOperatorType {
    EQUAL_OP,
    UNEQUAL_OP,
    LESS_THAN_OP,
    LESS_THAN_EQUAL_OP,
    GREATER_THAN_OP,
    GREATER_THAN_EQUAL_OP,
    PLUS_OP,
    MINUS_OP,
    MULTIPLY_OP,
    DIVISION_OP,
    MODULO_OP,
    POWER_OP,
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

#endif  // NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_OPERATOR_TYPES_HPP_
