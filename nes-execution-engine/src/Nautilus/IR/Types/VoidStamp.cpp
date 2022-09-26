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
#include <Nautilus/IR/Types/VoidStamp.hpp>

namespace NES::Nautilus::IR::Types {
VoidStamp::VoidStamp() : Stamp(&type) {}
const std::string VoidStamp::toString() const { return "void"; }

}// namespace NES::Nautilus::IR::Types