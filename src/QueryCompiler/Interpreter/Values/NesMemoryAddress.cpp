/*

     Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <QueryCompiler/Interpreter/Values/NesMemoryAddress.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt64.hpp>
#include <QueryCompiler/Interpreter/Values/NesIntU8.hpp>

namespace NES::QueryCompilation {

NesMemoryAddress::NesMemoryAddress(uint8_t* value) : ptr(value) {}


uint8_t* NesMemoryAddress::getValue() const { return ptr; }


NesValuePtr NesMemoryAddress::add(NesValuePtr other) const {
    if (auto v = std::dynamic_pointer_cast<NesInt64>(other)) {
        return std::make_shared<NesMemoryAddress>(ptr + v->getValue());
    }
    if (auto v = std::dynamic_pointer_cast<NesInt32>(other)) {
        return std::make_shared<NesMemoryAddress>(ptr + v->getValue());
    }
    return NesValue::add(other);
}
NesValuePtr NesMemoryAddress::mul(NesValuePtr other) const {
    if (auto v = std::dynamic_pointer_cast<NesInt64>(other)) {
        return std::make_shared<NesMemoryAddress>(ptr + v->getValue());
    }
    return NesValue::mul(other);
}
NesValuePtr NesMemoryAddress::sub(NesValuePtr other) const {
    if (auto v = std::dynamic_pointer_cast<NesInt64>(other)) {
        return std::make_shared<NesMemoryAddress>(ptr - v->getValue());
    }
    return NesValue::mul(other);
}

}// namespace NES::QueryCompilation