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

#include <QueryCompiler/Interpreter/Values/NesBool32.hpp>

namespace NES::QueryCompilation {

NesBool::NesBool(bool value) : value(value) {}

NesValuePtr NesBool::equals(NesValuePtr oValue) const {
    if (auto v = std::dynamic_pointer_cast<NesBool>(oValue)) {
        return std::make_shared<NesBool>(value == v->getValue());
    } else {
        throw std::exception();
    }
}
bool NesBool::getValue() { return value; }


}// namespace NES::QueryCompilation