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

#include <QueryCompiler/Interpreter/Values/NesInt64.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <QueryCompiler/Interpreter/Values/NesIntU8.hpp>
#include <QueryCompiler/Interpreter/Values/NesMemoryAddress.hpp>

namespace NES::QueryCompilation {

NesInt64::NesInt64(int64_t value) : value(value) {}

NesValuePtr NesInt64::equals(NesValuePtr oValue) const {
    if (auto v = std::dynamic_pointer_cast<NesInt64>(oValue)) {
        return std::make_shared<NesBool>(value == v->getValue());
    } else {
        throw std::exception();
    }
}

int64_t NesInt64::getValue() const { return value; }

void NesInt64::write(NesMemoryAddressPtr memoryAddress) const { *reinterpret_cast<int64_t*>(memoryAddress->getValue()) = value; }

NesValuePtr NesInt64::add(NesValuePtr ptr) const {
    if (auto v = std::dynamic_pointer_cast<NesInt64>(ptr)) {
        return std::make_shared<NesBool>(value + v->value);
    }
    if (auto v = std::dynamic_pointer_cast<NesInt32>(ptr)) {
        return std::make_shared<NesBool>(value + v->getValue());
    }
    return NesValue::add(ptr);
}
NesValuePtr NesInt64::mul(NesValuePtr ptr) const {
    if (auto v = std::dynamic_pointer_cast<NesInt64>(ptr)) {
        return std::make_shared<NesBool>(value + v->value);
    }
    return NesValue::mul(ptr);
}

}// namespace NES::QueryCompilation