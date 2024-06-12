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

#include <Common/PhysicalTypes/TextPhysicalType.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/core.h>

namespace NES {

uint64_t TextPhysicalType::size() const {
    // returning the size of the index to the child buffer that contains the text data
    return sizeof(uint32_t);
}

std::string TextPhysicalType::convertRawToString(void const* data) const noexcept {
    // We always read the exact number of bytes contained by the Text.
    return convertRawToStringWithoutFill(data);
}

std::string TextPhysicalType::convertRawToStringWithoutFill(void const* data) const noexcept {
    if (!data) {
        NES_ERROR("Pointer to variable sized data is invalid. Buffer must at least contain the length (0 if empty).");
        return "";
    }

    // Read the length of the Text from the first StringLengthType bytes from the buffer and adjust the data pointer.
    using StringLengthType = uint32_t;
    StringLengthType textLength = *static_cast<uint32_t const*>(data);
    const auto* textPointer = static_cast<char const*>(data);
    textPointer += sizeof(StringLengthType);
    if (!textPointer) {
        NES_ERROR("Pointer to text is invalid.");
        return "";
    }
    return std::string(textPointer, textLength);
}

std::string TextPhysicalType::toString() const noexcept { return "TEXT"; }

}// namespace NES
