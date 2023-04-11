#include "Nautilus/Backends/BCInterpreter/ByteCode.hpp"
#include <Nautilus/Backends/BCInterpreter/Dyncall.hpp>
#include <dyncall.h>

namespace NES::Nautilus::Backends::BC {

Dyncall::Dyncall() : vm(dcNewCallVM(VM_STACK_SIZE)) {}

void Dyncall::addArgB(bool value) { dcArgBool(vm, value); }
void Dyncall::addArgI8(int8_t value) { dcArgChar(vm, value); }
void Dyncall::addArgI16(int16_t value) { dcArgShort(vm, value); }
void Dyncall::addArgI32(int32_t value) { dcArgInt(vm, value); }
void Dyncall::addArgI64(int64_t value) { dcArgLong(vm, value); }
void Dyncall::addArgD(double value) { dcArgDouble(vm, value); }
void Dyncall::addArgF(float value) { dcArgFloat(vm, value); }
void Dyncall::addArgPtr(void* value) { dcArgPointer(vm, value); }

void Dyncall::callVoid(void* value) {
    dcCallVoid(vm, value);
}

bool Dyncall::callB(void* value) {
    return dcCallBool(vm, value);
}

int8_t Dyncall::callI8(void* value) {
    return dcCallChar(vm, value);
}

int16_t Dyncall::callI16(void* value) {
    return dcCallShort(vm, value);
}

int32_t Dyncall::callI32(void* value) {
    return dcCallInt(vm, value);
}

int64_t Dyncall::callI64(void* value) {
    return dcCallLong(vm, value);
}

float Dyncall::callF(void* value) {
    return dcCallFloat(vm, value);
}

double Dyncall::callD(void* value) {
    return dcCallDouble(vm, value);
}

void* Dyncall::callPtr(void* value) {
    return dcCallPointer(vm, value);
}
Dyncall& Dyncall::getVM() {
    static Dyncall vm;
    return vm;
}

void Dyncall::reset() {
    dcReset(vm);
}

}// namespace NES::Nautilus::Backends::BC