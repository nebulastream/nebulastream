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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACE_OPCODE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACE_OPCODE_HPP_
namespace NES::Nautilus::Tracing {
/**
 * @brief The OpCode enum defines the different traceable primitive operations.
 */
enum class OpCode : uint8_t {
    ADD,
    SUB,
    DIV,
    MOD,
    MUL,
    EQUALS,
    LESS_THAN,
    GREATER_THAN,
    NEGATE,
    BITWISE_AND,
    BITWISE_OR,
    BITWISE_XOR,
    BITWISE_LEFT_SHIFT,
    BITWISE_RIGHT_SHIFT,
    AND,
    OR,
    CMP,
    JMP,
    CONST,
    ASSIGN,
    RETURN,
    LOAD,
    STORE,
    CALL,
    CAST
};
}// namespace NES::Nautilus::Tracing
#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TRACE_OPCODE_HPP_
