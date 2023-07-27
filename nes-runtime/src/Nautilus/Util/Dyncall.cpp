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

#include <Nautilus/Util/Dyncall.hpp>
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

void Dyncall::callVoid(void* value) { dcCallVoid(vm, value); }

bool Dyncall::callB(void* value) { return dcCallBool(vm, value); }

int8_t Dyncall::callI8(void* value) { return dcCallChar(vm, value); }

int16_t Dyncall::callI16(void* value) { return dcCallShort(vm, value); }

int32_t Dyncall::callI32(void* value) { return dcCallInt(vm, value); }

int64_t Dyncall::callI64(void* value) { return dcCallLong(vm, value); }

float Dyncall::callF(void* value) { return dcCallFloat(vm, value); }

double Dyncall::callD(void* value) { return dcCallDouble(vm, value); }

void* Dyncall::callPtr(void* value) { return dcCallPointer(vm, value); }
Dyncall& Dyncall::getVM() {
    static Dyncall vm;
    return vm;
}

void Dyncall::reset() { dcReset(vm); }

}// namespace NES::Nautilus::Backends::BC