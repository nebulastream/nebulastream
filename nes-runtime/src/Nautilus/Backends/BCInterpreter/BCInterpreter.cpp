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

#include <Nautilus/Backends/BCInterpreter/BCInterpreter.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Nautilus::Backends::BC {
void regMov(const OpCode& c, RegisterFile& regs) { regs[c.output] = regs[c.reg1]; }

static Operation* OpTable[] = {
    (Operation*) regMov,
    (Operation*) add<int8_t>,
    (Operation*) add<int16_t>,
    (Operation*) add<int32_t>,
    (Operation*) add<int64_t>,
    (Operation*) add<uint8_t>,
    (Operation*) add<uint16_t>,
    (Operation*) add<uint32_t>,
    (Operation*) add<uint64_t>,
    (Operation*) add<float>,
    (Operation*) add<double>,
    (Operation*) sub<int8_t>,
    (Operation*) sub<int16_t>,
    (Operation*) sub<int32_t>,
    (Operation*) sub<int64_t>,
    (Operation*) sub<uint8_t>,
    (Operation*) sub<uint16_t>,
    (Operation*) sub<uint32_t>,
    (Operation*) sub<uint64_t>,
    (Operation*) sub<float>,
    (Operation*) sub<double>,
    (Operation*) mul<int8_t>,
    (Operation*) mul<int16_t>,
    (Operation*) mul<int32_t>,
    (Operation*) mul<int64_t>,
    (Operation*) mul<uint8_t>,
    (Operation*) mul<uint16_t>,
    (Operation*) mul<uint32_t>,
    (Operation*) mul<uint64_t>,
    (Operation*) mul<float>,
    (Operation*) mul<double>,
    (Operation*) div<int8_t>,
    (Operation*) div<int16_t>,
    (Operation*) div<int32_t>,
    (Operation*) div<int64_t>,
    (Operation*) div<uint8_t>,
    (Operation*) div<uint16_t>,
    (Operation*) div<uint32_t>,
    (Operation*) div<uint64_t>,
    (Operation*) div<float>,
    (Operation*) div<double>,
    (Operation*) equals<int8_t>,
    (Operation*) equals<int16_t>,
    (Operation*) equals<int32_t>,
    (Operation*) equals<int64_t>,
    (Operation*) equals<uint8_t>,
    (Operation*) equals<uint16_t>,
    (Operation*) equals<uint32_t>,
    (Operation*) equals<uint64_t>,
    (Operation*) equals<float>,
    (Operation*) equals<double>,
    (Operation*) equals<bool>,
    (Operation*) lessThan<int8_t>,
    (Operation*) lessThan<int16_t>,
    (Operation*) lessThan<int32_t>,
    (Operation*) lessThan<int64_t>,
    (Operation*) lessThan<uint8_t>,
    (Operation*) lessThan<uint16_t>,
    (Operation*) lessThan<uint32_t>,
    (Operation*) lessThan<uint64_t>,
    (Operation*) lessThan<float>,
    (Operation*) lessThan<double>,
    (Operation*) greaterThan<int8_t>,
    (Operation*) greaterThan<int16_t>,
    (Operation*) greaterThan<int32_t>,
    (Operation*) greaterThan<int64_t>,
    (Operation*) greaterThan<uint8_t>,
    (Operation*) greaterThan<uint16_t>,
    (Operation*) greaterThan<uint32_t>,
    (Operation*) greaterThan<uint64_t>,
    (Operation*) greaterThan<float>,
    (Operation*) greaterThan<double>,
    (Operation*) load<int8_t>,
    (Operation*) load<int16_t>,
    (Operation*) load<int32_t>,
    (Operation*) load<int64_t>,
    (Operation*) load<uint8_t>,
    (Operation*) load<uint16_t>,
    (Operation*) load<uint32_t>,
    (Operation*) load<uint64_t>,
    (Operation*) load<float>,
    (Operation*) load<double>,
    (Operation*) load<bool>,
    (Operation*) store<int8_t>,
    (Operation*) store<int16_t>,
    (Operation*) store<int32_t>,
    (Operation*) store<int64_t>,
    (Operation*) store<uint8_t>,
    (Operation*) store<uint16_t>,
    (Operation*) store<uint32_t>,
    (Operation*) store<uint64_t>,
    (Operation*) store<float>,
    (Operation*) store<double>,
    (Operation*) store<bool>,
    (Operation*) andOp<bool>,
    (Operation*) orOp<bool>,
    // FUNCTION CALLS
    // return void
    (Operation*) call<void>,
    (Operation*) call<void, void*>,
    /*CALL_v_ptr_ui64*/ (Operation*) call<void, void*, uint64_t>,
    /*CALL_v_ptr_ptr_ptr*/ (Operation*) call<void, void*, void*, void*>,
    /*CALL_v_ptr_ptr_ptr_ui64_ui64_ui64_ui64*/
    (Operation*) call<void, void*, void*, void*, uint64_t, uint64_t, uint64_t, uint64_t>,
    // Return uint64_t
    (Operation*) call<uint64_t, void*>,
    // Return int64_t
    (Operation*) call<int64_t>,
    (Operation*) call<int64_t, int64_t>,
    (Operation*) call<int64_t, int64_t, int64_t>,
    // Return ptr
    (Operation*) call<void*, void*>,
    (Operation*) call<void*, void*, void*>,
    /*CALL_ptr_ptr_i64*/
    (Operation*) call<void*, void*, int64_t>,
    /*CALL_ptr_ptr_ui64*/
    (Operation*) call<void*, void*, uint64_t>,
};

FunctionCallTarget::FunctionCallTarget(std::vector<std::pair<short, Type>> arguments, void* functionPtr)
    : arguments(std::move(arguments)), functionPtr(functionPtr) {}

BCInterpreter::BCInterpreter(Code code, RegisterFile registerFile) : code(std::move(code)), registerFile(registerFile) {}

class BCInvocable : public Executable::GenericInvocable {
  public:
    explicit BCInvocable(BCInterpreter& bcInterpreter) : bcInterpreter(bcInterpreter) {}
    std::any invokeGeneric(const std::vector<std::any>& vector) override { return bcInterpreter.invokeGeneric(vector); }

  private:
    BCInterpreter& bcInterpreter;
};

void* BCInterpreter::getInvocableFunctionPtr(const std::string&) { return (void*) nullptr; }

bool BCInterpreter::hasInvocableFunctionPtr() { return false; }

std::unique_ptr<Executable::GenericInvocable> BCInterpreter::getGenericInvocable(const std::string&) {
    return std::make_unique<BCInvocable>(*this);
}

std::any BCInterpreter::invokeGeneric(const std::vector<std::any>& args) {
    NES_ASSERT(args.size() == code.arguments.size(), "Arguments are not of the correct size");

    for (size_t i = 0; i < args.size(); i++) {
        if (auto* value = std::any_cast<int8_t>(&args[i])) {
            writeReg<>(registerFile, code.arguments[i], *value);
        } else if (auto* value = std::any_cast<int16_t>(&args[i])) {
            writeReg<>(registerFile, code.arguments[i], *value);
        } else if (auto* value = std::any_cast<int32_t>(&args[i])) {
            writeReg<>(registerFile, code.arguments[i], *value);
        } else if (auto* value = std::any_cast<int64_t>(&args[i])) {
            writeReg<>(registerFile, code.arguments[i], *value);
        } else if (auto* value = std::any_cast<float>(&args[i])) {
            writeReg<>(registerFile, code.arguments[i], *value);
        } else if (auto* value = std::any_cast<double>(&args[i])) {
            writeReg<>(registerFile, code.arguments[i], *value);
        } else if (auto* value = std::any_cast<void*>(&args[i])) {
            auto val = (int64_t) *value;
            registerFile[code.arguments[i]] = val;
        } else {
            NES_NOT_IMPLEMENTED();
        }
    }
    // set arguments
    auto result = execute(registerFile);
    switch (code.returnType) {
        case Type::v: return nullptr;
        case Type::i8: return (int8_t) result;
        case Type::i16: return (int16_t) result;
        case Type::i32: return (int32_t) result; ;
        case Type::i64: return (int64_t) result; ;
        case Type::ui8: return (uint8_t) result;
        case Type::ui16: return (uint16_t) result;
        case Type::ui32: return (uint32_t) result; ;
        case Type::ui64: return (uint64_t) result; ;
        case Type::d: return *reinterpret_cast<double*>(&result); ;
        case Type::f: return *reinterpret_cast<float*>(&result); ;
        case Type::b: return (bool) result; ;
        case Type::ptr: return (void*) result; ;
    }
}
int64_t BCInterpreter::execute(RegisterFile& regs) const {
    // first block is always the entrypoint
    auto* currentBlock = &code.blocks[0];
    while (true) {
        // execute operations in block
        for (const auto& c : currentBlock->code) {
            OpTable[(int16_t) c.op](c, regs);
        }
        // handle terminator
        if (const auto* res = std::get_if<BranchOp>(&currentBlock->terminatorOp)) {
            currentBlock = &code.blocks[res->nextBlock];
        } else if (const auto* res = std::get_if<ConditionalJumpOp>(&currentBlock->terminatorOp)) {
            if (readReg<bool>(regs, res->conditionalReg)) {
                currentBlock = &code.blocks[res->trueBlock];
            } else {
                currentBlock = &code.blocks[res->falseBlock];
            }
        } else if (const auto* res = std::get_if<ReturnOp>(&currentBlock->terminatorOp)) {
            return regs[res->resultReg];
        }
    }
}

}// namespace NES::Nautilus::Backends::BC