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

#include <QueryCompiler/Interpreter/Values/NesValue.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>
#include <QueryCompiler/Interpreter/Values/NesInt64.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::QueryCompilation {

NesValuePtr NesValue::add(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::div(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::mul(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::sub(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::le(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::lt(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::ge(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::gt(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
NesValuePtr NesValue::equals(NesValuePtr) const { NES_NOT_IMPLEMENTED(); }
void NesValue::write(NesMemoryAddressPtr) const { NES_NOT_IMPLEMENTED(); }

NesValuePtr createNesInt(int32_t v){
    return std::make_shared<NesInt32>(v);
}

NesValuePtr createNesLong(int64_t){
    return std::make_shared<NesInt32>(10);
}

}// namespace NES::QueryCompilation