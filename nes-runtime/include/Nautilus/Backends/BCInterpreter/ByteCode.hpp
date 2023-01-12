#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_

// Type your code here, or load an example.
#include <array>
#include <cstdint>

std::array<int64_t, 256> regs;

enum struct ByteCode : short {
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
    RETURN
};
struct OpCode;
typedef void Operation(OpCode&);

struct OpCode {
    ByteCode op;
    short reg1;
    short reg2;
    short output;
};

template<class x>
void add(const OpCode& c) {
    auto l = (x) regs[c.reg1];
    auto r = (x) regs[c.reg2];
    regs[c.output] = l + r;
}

template<class x>
void sub(const OpCode& c) {
    auto l = (x) regs[c.reg1];
    auto r = (x) regs[c.reg2];
    regs[c.output] = l - r;
}

template<class x>
void mul(const OpCode& c) {
    auto l = (x) regs[c.reg1];
    auto r = (x) regs[c.reg2];
    regs[c.output] = l * r;
}

template<class x>
void div(const OpCode& c) {
    auto l = (x) regs[c.reg1];
    auto r = (x) regs[c.reg2];
    regs[c.output] = l / r;
}

static Operation* OpTable[] = {
    (Operation*) add<int8_t>,   (Operation*) add<int16_t>,  (Operation*) add<int32_t>,  (Operation*) add<int64_t>,
    (Operation*) add<uint8_t>,  (Operation*) add<uint16_t>, (Operation*) add<uint32_t>, (Operation*) add<uint64_t>,
    (Operation*) add<float>,    (Operation*) add<double>,   (Operation*) sub<int8_t>,   (Operation*) sub<int16_t>,
    (Operation*) sub<int32_t>,  (Operation*) sub<int64_t>,  (Operation*) sub<uint8_t>,  (Operation*) sub<uint16_t>,
    (Operation*) sub<uint32_t>, (Operation*) sub<uint64_t>, (Operation*) sub<float>,    (Operation*) sub<double>,
    (Operation*) mul<int8_t>,   (Operation*) mul<int16_t>,  (Operation*) mul<int32_t>,  (Operation*) mul<int64_t>,
    (Operation*) mul<uint8_t>,  (Operation*) mul<uint16_t>, (Operation*) mul<uint32_t>, (Operation*) mul<uint64_t>,
    (Operation*) mul<float>,    (Operation*) mul<double>,   (Operation*) div<int8_t>,   (Operation*) div<int16_t>,
    (Operation*) div<int32_t>,  (Operation*) div<int64_t>,  (Operation*) div<uint8_t>,  (Operation*) div<uint16_t>,
    (Operation*) div<uint32_t>, (Operation*) div<uint64_t>, (Operation*) div<float>,    (Operation*) div<double>,
};

void execute(int32_t length, OpCode* code) {
    for (int32_t i = 0; i < length; i++) {
        auto& c = code[i];
        OpTable[(int16_t) c.op](c);
    }
}

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_BYTECODE_HPP_
