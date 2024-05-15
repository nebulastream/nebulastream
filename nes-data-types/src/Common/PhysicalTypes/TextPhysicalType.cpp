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
#include <fmt/core.h>
#include <Util/Logger/Logger.hpp>

namespace NES {

// Todo: size of the index or the string length? -> how to determine?
uint64_t TextPhysicalType::size() const {
    // returning the size of the index to the child buffer that contains the text data
    return sizeof(uint32_t);
}

std::string TextPhysicalType::convertRawToString(void const* data) const noexcept {
    return convertRawToStringWithoutFill(data);
//    const auto* dataC = static_cast<char const*>(data);
//    // check if the pointer is valid
//    if (!data) {
//        return "";
//    }
//    return std::string(dataC, size());
    // we print a fixed char directly because the last char terminated the output.
//    if (physicalComponentType->type->isChar()) {
        // This char is fixed size, so we have to convert it to a fixed size string.
        // Otherwise, we would copy all data till the termination character.
//        return std::string(dataC, size());
//    }

//    std::stringstream str;
//    str << '[';
//    for (uint64_t dimension = 0; dimension < length; ++dimension) {
//        if (dimension) {
//            str << ", ";
//        }
//        auto const fieldOffset = physicalComponentType->size();
//        const auto* const componentValue = &dataC[fieldOffset * dimension];
//        str << physicalComponentType->convertRawToString(componentValue);
//    }
//    str << ']';
//    return str.str();
}

std::string TextPhysicalType::convertRawToStringWithoutFill(void const* data) const noexcept {
    // Todo: add support beyond just text and add support for multidimensional data
    // check if the pointer is valid
    if (!data) {
        NES_WARNING("Pointer to variable sized data is invalid.")
        return "";
    }
    using StringLengthType = uint32_t;
    StringLengthType textLength = *static_cast<uint32_t const*>(data);
    const auto* dataC = static_cast<char const*>(data);
    dataC += sizeof(StringLengthType);

//    NES_ASSERT(textLength == size(), fmt::format("Length of TEXT written to buffer must equal size of buffer. {} != {}", textLength, size()));
    return std::string(dataC, textLength);
//    const auto* dataC = static_cast<char const*>(data);
//    // check if the pointer is valid
//    if (!dataC) {
//        return "";
//    }
//    // we print a fixed char directly because the last char terminated the output.
//    if (physicalComponentType->type->isChar()) {
//        // Only copy the actual content of the char. If the size is larger than the schema definition
//        // only copy until the defined size of the schema
//        if (std::string(dataC).length() < size()) {
//            return std::string(dataC);
//        } else {
//            return std::string(dataC, size());
//        }
//    }
//
//    std::stringstream str;
//    str << '[';
//    for (uint64_t dimension = 0; dimension < length; ++dimension) {
//        if (dimension) {
//            str << ", ";
//        }
//        auto const fieldOffset = physicalComponentType->size();
//        const auto* const componentValue = &dataC[fieldOffset * dimension];
//        str << physicalComponentType->convertRawToString(componentValue);
//    }
//    str << ']';
//    return str.str();
}

std::string TextPhysicalType::toString() const noexcept {
    return "TEXT";
}
}// namespace NES
