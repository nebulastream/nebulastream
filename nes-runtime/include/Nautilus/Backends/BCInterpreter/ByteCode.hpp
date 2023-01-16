#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_

#include <any>
#include <array>
#include <cstdint>
#include <variant>
#include <vector>

namespace NES::Nautilus::Backends::BC {
// Type your code here, or load an example.

using RegisterFile = std::array<int64_t, 256>;
enum struct ByteCode : short {
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
    // Function calls TODO come up with a better approach to call dynamically into runtime functions
    // functions with void return
    CALL_v,
    // functions with i64 return
    CALL_i64,
    CALL_i64_i64,
    CALL_i64_i64_i64
};

enum Type : uint8_t { v, i8, i16, i32, i64, d, f, b, ptr };

struct OpCode;
typedef void Operation(const OpCode&, RegisterFile& regs);

class FunctionCallTarget {
  public:
    FunctionCallTarget(std::vector<std::pair<short, Type>> arguments, void* functionPtr);
    std::vector<std::pair<short, Type>> arguments;
    void* functionPtr;
};

struct OpCode {
    ByteCode op;
    short reg1;
    short reg2;
    short output;
};

template<class x>
auto inline readReg(RegisterFile& regs, short reg) {
    return *reinterpret_cast<x*>(&regs[reg]);
}

template<class x>
void inline writeReg(RegisterFile& regs, short reg, x value) {
    *reinterpret_cast<x*>(&regs[reg]) = value;
}

void regMov(const OpCode& c, RegisterFile& regs);

template<typename X, typename... Args>
void call(const OpCode& c, RegisterFile& regs) {
    auto address = readReg<int64_t>(regs, c.reg1);
    auto fcall = reinterpret_cast<FunctionCallTarget*>(address);
    auto x = (X(*)(Args...)) fcall->functionPtr;

    if constexpr (std::is_void_v<X>) {
        if constexpr (sizeof...(Args) == 0) {
            x();
        } else if constexpr (sizeof...(Args) == 1) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            x(value0);
        } else if constexpr (sizeof...(Args) == 2) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            auto value1 = readReg<__type_pack_element<1, Args...>>(regs, fcall->arguments[1].first);
            x(value0, value1);
        }
    } else {
        X rValue;
        if constexpr (sizeof...(Args) == 0) {
            rValue = x();
        } else if constexpr (sizeof...(Args) == 1) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            rValue = x(value0);
        } else if constexpr (sizeof...(Args) == 2) {
            auto value0 = readReg<__type_pack_element<0, Args...>>(regs, fcall->arguments[0].first);
            auto value1 = readReg<__type_pack_element<1, Args...>>(regs, fcall->arguments[1].first);
            rValue = x(value0, value1);
        }
        writeReg(regs, c.output, rValue);
    }
}

template<class x>
void add(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<x>(regs, c.reg1);
    auto r = readReg<x>(regs, c.reg2);
    writeReg(regs, c.output, l + r);
}

template<class x>
void sub(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<x>(regs, c.reg1);
    auto r = readReg<x>(regs, c.reg2);
    writeReg(regs, c.output, l - r);
}

template<class x>
void mul(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<x>(regs, c.reg1);
    auto r = readReg<x>(regs, c.reg2);
    writeReg(regs, c.output, l * r);
}

template<class x>
void div(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<x>(regs, c.reg1);
    auto r = readReg<x>(regs, c.reg2);
    writeReg(regs, c.output, l / r);
}

template<class x>
void equals(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<x>(regs, c.reg1);
    auto r = readReg<x>(regs, c.reg2);
    writeReg(regs, c.output, l == r);
}

template<class x>
void lessThan(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<x>(regs, c.reg1);
    auto r = readReg<x>(regs, c.reg2);
    writeReg(regs, c.output, l < r);
}

template<class x>
void greaterThan(const OpCode& c, RegisterFile& regs) {
    auto l = readReg<x>(regs, c.reg1);
    auto r = readReg<x>(regs, c.reg2);
    writeReg(regs, c.output, l > r);
}

template<class x>
void load(const OpCode& c, RegisterFile& regs) {
    auto address = readReg<int64_t>(regs, c.reg1);
    auto ptr = reinterpret_cast<x*>(address);
    writeReg(regs, c.output, *ptr);
}

template<class x>
void store(const OpCode& c, RegisterFile& regs) {
    auto address = readReg<int64_t>(regs, c.reg1);
    auto value = readReg<x>(regs, c.reg2);
    *reinterpret_cast<x*>(address) = value;
}

struct JumpOp {
    short nextBlock;
};

struct ConditionalJumpOp {
    short conditionalReg;
    short trueBlock;
    short falseBlock;
};

struct ReturnOp {
    short resultReg;
};

class CodeBlock {
  public:
    CodeBlock() = default;
    std::vector<OpCode> code = std::vector<OpCode>();
    std::variant<JumpOp, ConditionalJumpOp, ReturnOp> terminatorOp = ReturnOp{0};
};

class Code {
  public:
    Code() = default;
    std::vector<short> arguments = std::vector<short>();
    std::vector<CodeBlock> blocks = std::vector<CodeBlock>();
    Type returnType = v;
};

}// namespace NES::Nautilus::Backends::BC

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_
