#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
#include <string>
namespace NES::Nautilus::Backends {

class Executable {
  public:
    template<typename Function>
    Function getInvocableMember(const std::string& member) {
        return reinterpret_cast<Function>(getInvocableFunctionPtr(member));
    }
    virtual ~Executable() = default;

  protected:
    /**
     * @brief Returns a untyped function pointer to a specific symbol.
     * @param member on the dynamic object, currently provided as a MangledName.
     * @return function ptr
     */
    [[nodiscard]] virtual void* getInvocableFunctionPtr(const std::string& member) = 0;
};

}// namespace NES::Nautilus::Backends
#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_EXECUTABLE_HPP_
