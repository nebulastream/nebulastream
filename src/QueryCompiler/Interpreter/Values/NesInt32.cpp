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

#include <QueryCompiler/Interpreter/Values/NesInt32.hpp>

namespace NES::QueryCompilation {

NesInt32::NesInt32(int32_t value) : value(value) {}

NesValuePtr NesInt32::equals(NesValuePtr oValue) const {
    if (auto v = std::dynamic_pointer_cast<NesInt32>(oValue)) {
        return std::make_shared<NesBool>(value == v->getValue());
    } else {
        throw std::exception();
    }
}

int32_t NesInt32::getValue() const { return value; }

}// namespace NES::QueryCompilation