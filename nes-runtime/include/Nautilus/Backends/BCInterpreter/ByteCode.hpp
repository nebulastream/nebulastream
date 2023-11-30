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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_

#include <any>
#include <array>
#include <cstdint>
#include <ostream>
#include <variant>
#include <vector>

namespace NES::Nautilus::Backends::BC {

/**
 * @brief This defines the central register file for the byte-code interpreter.
 * In the current version we only support 1024 registers at max.
 */
constexpr short REGISTERS = 1024;
using RegisterFile = std::array<int64_t, REGISTERS>;

/**
 * @brief Defines an enum of all byte codes.
 */
enum class ByteCode : short {
    REG_MOV,
    // ADD
    ADD_i8,
    ADD_i16,
    ADD_i32,
    ADD_i64,
    ADD_ui8,
    ADD_ui16,
    ADD_ui32,
    ADD_ui64,
    ADD_f,
    ADD_d,
    // SUB
    SUB_i8,
    SUB_i16,
    SUB_i32,
    SUB_i64,
    SUB_ui8,
    SUB_ui16,
    SUB_ui32,
    SUB_ui64,
    SUB_f,
    SUB_d,
    // MUL
    MUL_i8,
    MUL_i16,
    MUL_i32,
    MUL_i64,
    MUL_ui8,
    MUL_ui16,
    MUL_ui32,
    MUL_ui64,
    MUL_f,
    MUL_d,
    // DIV
    DIV_i8,
    DIV_i16,
    DIV_i32,
    DIV_i64,
    DIV_ui8,
    DIV_ui16,
    DIV_ui32,
    DIV_ui64,
    DIV_f,
    DIV_d,
    // Equal
    EQ_i8,
    EQ_i16,
    EQ_i32,
    EQ_i64,
    EQ_ui8,
    EQ_ui16,
    EQ_ui32,
    EQ_ui64,
    EQ_f,
    EQ_d,
    EQ_b,
    // LessThan
    LESS_THAN_i8,
    LESS_THAN_i16,
    LESS_THAN_i32,
    LESS_THAN_i64,
    LESS_THAN_ui8,
    LESS_THAN_ui16,
    LESS_THAN_ui32,
    LESS_THAN_ui64,
    LESS_THAN_f,
    LESS_THAN_d,
    // greater than
    GREATER_THAN_i8,
    GREATER_THAN_i16,
    GREATER_THAN_i32,
    GREATER_THAN_i64,
    GREATER_THAN_ui8,
    GREATER_THAN_ui16,
    GREATER_THAN_ui32,
    GREATER_THAN_ui64,
    GREATER_THAN_f,
    GREATER_THAN_d,
    // Load
    LOAD_i8,
    LOAD_i16,
    LOAD_i32,
    LOAD_i64,
    LOAD_ui8,
    LOAD_ui16,
    LOAD_ui32,
    LOAD_ui64,
    LOAD_f,
    LOAD_d,
    LOAD_b,
    // Store
    STORE_i8,
    STORE_i16,
    STORE_i32,
    STORE_i64,
    STORE_ui8,
    STORE_ui16,
    STORE_ui32,
    STORE_ui64,
    STORE_f,
    STORE_d,
    STORE_b,
    // AND
    AND_b,
    // OR
    OR_b,
    // Negate
    NOT_b,
    // Cast
    CAST_i8_i16,
    CAST_i8_i32,
    CAST_i8_i64,
    CAST_i16_i32,
    CAST_i16_i64,
    CAST_i32_i64,
    CAST_ui8_i16,
    CAST_ui8_i32,
    CAST_ui8_i64,
    CAST_ui16_i32,
    CAST_ui16_i64,
    CAST_ui32_i64,
    CAST_ui8_ui16,
    CAST_ui8_ui32,
    CAST_ui8_ui64,
    CAST_ui16_ui32,
    CAST_ui16_ui64,
    CAST_ui32_ui64,
    CAST_i8_ui8,
    CAST_i8_ui16,
    CAST_i8_ui32,
    CAST_i8_ui64,
    CAST_i16_ui16,
    CAST_i16_ui32,
    CAST_i16_ui64,
    CAST_i32_ui32,
    CAST_i32_ui64,
    CAST_i64_ui64,
    CAST_f_d,
    CAST_i8_f,
    CAST_i8_d,
    CAST_i16_f,
    CAST_i16_d,
    CAST_i32_f,
    CAST_i32_d,
    CAST_i64_f,
    CAST_i64_d,
    // Function calls
    // TODO #3466 come up with a better approach to call dynamically into runtime functions
    // functions with void return
    CALL_v,
    CALL_v_ptr,
    CALL_v_ptr_ui64,
    CALL_v_ptr_ptr_ptr,
    CALL_v_ptr_ptr_ptr_ui64_ui64_ui64_ui64,
    // functions with ui64 return
    CALL_ui64_ptr,
    CALL_ui64_ui64_i8,
    CALL_ui64_ui64_i32,
    CALL_ui64_ui64_i64,
    // functions with i64 return
    CALL_i64,
    CALL_i64_i64,
    CALL_i64_i64_i64,
    // functions with ptr return
    CALL_ptr_ptr,
    CALL_ptr_ptr_ptr,
    CALL_ptr_ptr_i64,
    CALL_ptr_ptr_ui64,
    // dynamic function calls using dyncall.h
    DYNCALL_reset,
    DYNCALL_arg_b,
    DYNCALL_arg_i8,
    DYNCALL_arg_i16,
    DYNCALL_arg_i32,
    DYNCALL_arg_i64,
    DYNCALL_arg_f,
    DYNCALL_arg_d,
    DYNCALL_arg_ptr,
    DYNCALL_call_v,
    DYNCALL_call_b,
    DYNCALL_call_i8,
    DYNCALL_call_i16,
    DYNCALL_call_i32,
    DYNCALL_call_i64,
    DYNCALL_call_ptr
};

enum class Type : uint8_t { v, i8, i16, i32, i64, ui8, ui16, ui32, ui64, d, f, b, ptr };

/**
 * @brief Defines a function call target, that contains all information's necessary to call a function
 */
class FunctionCallTarget {
  public:
    FunctionCallTarget(std::vector<std::pair<short, Type>> arguments, void* functionPtr);
    std::vector<std::pair<short, Type>> arguments;
    void* functionPtr;
};

/**
 * @brief The general definition of opcode, that contains a bytecode, at max two input registers and a result register.
 */
struct OpCode {
    OpCode(ByteCode op, short reg1, short reg2, short output) : op(op), reg1(reg1), reg2(reg2), output(output){};
    ByteCode op;
    short reg1;
    short reg2;
    short output;
    friend std::ostream& operator<<(std::ostream& os, const OpCode& code);
};

typedef void Operation(const OpCode&, RegisterFile& regs);

template<class RegisterType>
auto inline readReg(RegisterFile& regs, short reg) {
    return *reinterpret_cast<RegisterType*>(&regs[reg]);
}

template<class RegisterType>
void inline writeReg(RegisterFile& regs, short reg, RegisterType value) {
    *reinterpret_cast<RegisterType*>(&regs[reg]) = value;
}

void regMov(const OpCode& c, RegisterFile& regs);

/**
 * @brief Defines a call operation for our bytecode interpreter.
 * Currently we only support external function calls with a fixed set of known signatures.
 * This should be generalized in #3466
 * @tparam ReturnType
 * @tparam Args
 * @param c
 * @param regs
 */
template<typename ReturnType, typename... Args>
void call(const OpCode& c, RegisterFile& regs) {
    auto address = readReg<int64_t>(regs, c.reg1);
    auto fcall = reinterpret_cast<FunctionCallTarget*>(address);
    auto ptr = (ReturnType(*)(Args...)) fcall->functionPtr;

    if constexpr (std::is_void_v<ReturnType>) {
        // if ReturnType is void we dont return a result.
        if constexpr (sizeof...(Args) == 0) {
            ptr();
        } else if constexpr (sizeof...(Args) == 1) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            ptr(value0);
        } else if constexpr (sizeof...(Args) == 2) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            auto value1 = readReg<__type_pack_element<1, Args...>>(regs, fcall->arguments[1].first);
            ptr(value0, value1);
        } else if constexpr (sizeof...(Args) == 3) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            auto value1 = readReg<__type_pack_element<1, Args...>>(regs, fcall->arguments[1].first);
            auto value2 = readReg<__type_pack_element<2, Args...>>(regs, fcall->arguments[2].first);
            ptr(value0, value1, value2);
        } else if constexpr (sizeof...(Args) == 7) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            auto value1 = readReg<__type_pack_element<1, Args...>>(regs, fcall->arguments[1].first);
            auto value2 = readReg<__type_pack_element<2, Args...>>(regs, fcall->arguments[2].first);
            auto value3 = readReg<__type_pack_element<3, Args...>>(regs, fcall->arguments[3].first);
            auto value4 = readReg<__type_pack_element<4, Args...>>(regs, fcall->arguments[4].first);
            auto value5 = readReg<__type_pack_element<5, Args...>>(regs, fcall->arguments[5].first);
            auto value6 = readReg<__type_pack_element<6, Args...>>(regs, fcall->arguments[6].first);
            ptr(value0, value1, value2, value3, value4, value5, value6);
        }
    } else {
        // the function has a return type. So we return a result.
        ReturnType returnValue;
        if constexpr (sizeof...(Args) == 0) {
            returnValue = ptr();
        } else if constexpr (sizeof...(Args) == 1) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            returnValue = ptr(value0);
        } else if constexpr (sizeof...(Args) == 2) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            auto value1 = readReg<__type_pack_element<1, Args...>>(regs, fcall->arguments[1].first);
            returnValue = ptr(value0, value1);
        } else if constexpr (sizeof...(Args) == 3) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            auto value1 = readReg<__type_pack_element<1, Args...>>(regs, fcall->arguments[1].first);
            auto value2 = readReg<__type_pack_element<2, Args...>>(regs, fcall->arguments[2].first);
            returnValue = ptr(value0, value1, value2);
        }
        writeReg(regs, c.output, returnValue);
    }
}
/**
 * @brief Defines an and in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void andOp(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l && r);
}

/**
 * @brief Defines an or in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void orOp(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l || r);
}

/**
 * @brief Defines an not in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void notOp(const OpCode& c, RegisterFile& regs) {
    auto value = readReg<RegisterType>(regs, c.reg1);
    writeReg(regs, c.output, !value);
}

/**
 * @brief Defines an add in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void add(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l + r);
}

/**
 * @brief Defines an sub in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void sub(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l - r);
}

/**
 * @brief Defines an mul in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void mul(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l * r);
}

/**
 * @brief Defines a div in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void div(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l / r);
}

/**
 * @brief Defines an equals in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void equals(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l == r);
}

/**
 * @brief Defines an less than in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void lessThan(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l < r);
}

/**
 * @brief Defines an greater than in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void greaterThan(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<RegisterType>(regs, c.reg1);
    auto r = readReg<RegisterType>(regs, c.reg2);
    writeReg(regs, c.output, l > r);
}

/**
 * @brief Defines an load in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void load(const OpCode& c, RegisterFile& regs) {
    auto address = readReg<int64_t>(regs, c.reg1);
    auto ptr = reinterpret_cast<RegisterType*>(address);
    writeReg(regs, c.output, *ptr);
}

/**
 * @brief Defines an store in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class RegisterType>
void store(const OpCode& c, RegisterFile& regs) {
    auto address = readReg<int64_t>(regs, c.reg1);
    auto value = readReg<RegisterType>(regs, c.reg2);
    *reinterpret_cast<RegisterType*>(address) = value;
}

/**
 * @brief Defines a cast in the bytecode interpreter
 * @tparam RegisterType
 * @param c
 * @param regs
 */
template<class SrcRegisterType, class TargetRegisterType>
void cast(const OpCode& c, RegisterFile& regs) {
    auto src = readReg<SrcRegisterType>(regs, c.reg1);
    TargetRegisterType value = src;
    writeReg<TargetRegisterType>(regs, c.output, value);
}

/**
 * @brief Defines a final branch terminator operation
 */
struct BranchOp {
    short nextBlock;
};

/**
 * @brief Defines a condition jump
 */
struct ConditionalJumpOp {
    short conditionalReg;
    short trueBlock;
    short falseBlock;
};

/**
 * @brief Defines a return
 */
struct ReturnOp {
    short resultReg;
};

class CodeBlock {
  public:
    CodeBlock() = default;
    std::vector<OpCode> code = std::vector<OpCode>();
    std::variant<BranchOp, ConditionalJumpOp, ReturnOp> terminatorOp = ReturnOp{0};
    friend std::ostream& operator<<(std::ostream& os, const CodeBlock& block);
};

class Code {
  public:
    Code() = default;
    std::vector<short> arguments = std::vector<short>();
    std::vector<CodeBlock> blocks = std::vector<CodeBlock>();
    Type returnType = Type::v;
    friend std::ostream& operator<<(std::ostream& os, const Code& code);
    std::string toString();
};

}// namespace NES::Nautilus::Backends::BC

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_
