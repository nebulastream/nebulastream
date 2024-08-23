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

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/std/cstring.h>

namespace NES::Nautilus {

VariableSizedData::VariableSizedData(const MemRefVal& content, const UInt32Val& size)
    : size(size), content(content) {}

VariableSizedData::VariableSizedData(const MemRefVal& pointerToVarSized)
    : VariableSizedData(pointerToVarSized, readValueFromMemRef(pointerToVarSized, uint32_t)) {}

VariableSizedData::VariableSizedData(const VariableSizedData& other) = default;
VariableSizedData& VariableSizedData::operator=(const VariableSizedData& other) noexcept {
    size = other.size;
    content = other.content;
    return *this;
}

BooleanVal operator==(const VariableSizedData& varSizedData, const BooleanVal& other) {
    return varSizedData.operator!() == other;
}

BooleanVal operator==(const BooleanVal& other, const VariableSizedData& varSizedData) {
    return varSizedData.operator!() == other;
}


BooleanVal VariableSizedData::operator==(const VariableSizedData& rhs) const {
    if (size != rhs.size) {
        return {false};
    }

    const auto compareResult = (nautilus::memcmp(content, rhs.content, size) == 0);
    return {compareResult};
}

BooleanVal VariableSizedData::operator!=(const VariableSizedData& rhs) const { return !(*this == rhs); }
[[nodiscard]] UInt32Val VariableSizedData::getSize() const { return size; }
[[nodiscard]] MemRefVal VariableSizedData::getContent() const { return content + UInt64Val(sizeof(uint32_t)); }
[[nodiscard]] MemRefVal VariableSizedData::getReference() const { return content; }
BooleanVal VariableSizedData::operator!() const { return size > 0 && content != nullptr; }


[[nodiscard]] std::ostream& VariableSizedData::operator<<(std::ostream& os) const {
    os << "Size(" << size.toString() << ")";
    /// Once https://github.com/nebulastream/nautilus/pull/27 is merged, we can print out the content via hex
    return os;
}
}// namespace NES::Nautilus
