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
#include <Nautilus/IR/Types/FloatStamp.hpp>

namespace NES::Nautilus::IR::Types {

FloatStamp::FloatStamp(BitWidth bitWidth) : Stamp(&type), bitWidth(bitWidth) {}

FloatStamp::BitWidth FloatStamp::getBitWidth() const { return bitWidth; }

uint32_t FloatStamp::getNumberOfBits() const {
    switch (getBitWidth()) {
        case BitWidth::F32: return 32;
        case BitWidth::F64: return 64;
    }
}

const std::string FloatStamp::toString() const { return "f" + std::to_string(getNumberOfBits()); }

}// namespace NES::Nautilus::IR::Types