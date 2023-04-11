
#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_DYNCALL_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_DYNCALL_HPP_

#include <cinttypes>
#include <cstdint>

typedef struct DCCallVM_    DCCallVM;
namespace NES::Nautilus::Backends::BC {

class Dyncall {
  public:
    static constexpr uint64_t VM_STACK_SIZE = 4096;
    Dyncall();
    static Dyncall& getVM();
    void reset();
    void addArgB(bool value);
    void addArgI8(int8_t value);
    void addArgI16(int16_t value);
    void addArgI32(int32_t value);
    void addArgI64(int64_t value);
    void addArgD(double value);
    void addArgF(float value);
    void addArgPtr(void* value);
    void callVoid(void* value);
    bool callB(void* value);
    int8_t callI8(void* value);
    int16_t callI16(void* value);
    int32_t callI32(void* value);
    int64_t callI64(void* value);
    double callD(void* value);
    float callF(void* value);
    void* callPtr(void* value);

  private:
    DCCallVM* vm;
};
}// namespace NES::Nautilus::Backends::BC

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_BCINTERPRETER_DYNCALL_HPP_
