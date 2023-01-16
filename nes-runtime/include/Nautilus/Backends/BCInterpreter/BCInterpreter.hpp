#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_INTERPRETER_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_INTERPRETER_HPP_
#include <Nautilus/Backends/BCInterpreter/ByteCode.hpp>
#include <Nautilus/Backends/Executable.hpp>
namespace NES::Nautilus::Backends::BC {

class BCInterpreter : public Executable {
  public:
    BCInterpreter(Code code, RegisterFile registerFile);
    ~BCInterpreter() override = default;

  public:
    void* getInvocableFunctionPtr(const std::string& member) override;
    bool hasInvocableFunctionPtr() override;
    std::any invokeGeneric(const std::string& string, const std::vector<std::any>& arguments) override;

  private:
    int64_t execute(RegisterFile& regs) const;
    Code code;
    RegisterFile registerFile;
};
}// namespace NES::Nautilus::Backends::BC

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_INTERPRETER_HPP_
