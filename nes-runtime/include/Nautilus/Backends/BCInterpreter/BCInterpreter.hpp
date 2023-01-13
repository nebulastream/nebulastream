#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_INTERPRETER_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_INTERPRETER_HPP_
#include <Nautilus/Backends/Executable.hpp>
namespace NES::Nautilus::Backends::BC {
class BCInterpreter : public Executable {
  public:
    ~BCInterpreter() override = default;

  public:
    void* getInvocableFunctionPtr(const std::string& member) override;
};
}// namespace NES::Nautilus::Backends::BC

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_INTERPRETER_HPP_
