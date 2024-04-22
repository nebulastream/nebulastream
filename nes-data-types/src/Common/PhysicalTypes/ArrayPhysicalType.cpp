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

#include <Common/PhysicalTypes/ArrayPhysicalType.hpp>
#include <iostream>
#include <sstream>

namespace NES {

uint64_t ArrayPhysicalType::size() const { return physicalComponentType->size() * length; }

bool ArrayPhysicalType::isCharArrayType() const noexcept { return type->isChar(); }

std::string ArrayPhysicalType::convertRawToString(void const* data) const noexcept {
    const auto* dataC = static_cast<char const*>(data);
    // check if the pointer is valid
    if (!data) {
        return "";
    }
    // we print a fixed char directly because the last char terminated the output.
    if (physicalComponentType->type->isChar()) {
        // This char is fixed size, so we have to convert it to a fixed size string.
        // Otherwise, we would copy all data till the termination character.
        return std::string(dataC, size());
    }

    std::stringstream str;
    str << '[';
    for (uint64_t dimension = 0; dimension < length; ++dimension) {
        if (dimension) {
            str << ", ";
        }
        auto const fieldOffset = physicalComponentType->size();
        const auto* const componentValue = &dataC[fieldOffset * dimension];
        str << physicalComponentType->convertRawToString(componentValue);
    }
    str << ']';
    return str.str();
}

std::string ArrayPhysicalType::convertRawToStringWithoutFill(void const* data) const noexcept {
    const auto* dataC = static_cast<char const*>(data);
    // check if the pointer is valid
    if (!dataC) {
        return "";
    }
    // we print a fixed char directly because the last char terminated the output.
    if (physicalComponentType->type->isChar()) {
        // Only copy the actual content of the char. If the size is larger than the schema definition
        // only copy until the defined size of the schema
        if (std::string(dataC).length() < size()) {
            return std::string(dataC);
        } else {
            return std::string(dataC, size());
        }
    }

    std::stringstream str;
    str << '[';
    for (uint64_t dimension = 0; dimension < length; ++dimension) {
        if (dimension) {
            str << ", ";
        }
        auto const fieldOffset = physicalComponentType->size();
        const auto* const componentValue = &dataC[fieldOffset * dimension];
        str << physicalComponentType->convertRawToString(componentValue);
    }
    str << ']';
    return str.str();
}

std::string ArrayPhysicalType::toString() const noexcept {
    std::stringstream sstream;
    sstream << physicalComponentType->toString() << '[' << length << ']';
    return sstream.str();
}
}// namespace NES