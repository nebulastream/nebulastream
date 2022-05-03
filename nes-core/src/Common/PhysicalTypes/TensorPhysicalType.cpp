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

#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/TensorPhysicalType.hpp>
#include <sstream>

namespace NES {

uint64_t TensorPhysicalType::size() const { return physicalComponentType->size() * totalSize; }

std::string TensorPhysicalType::convertRawToString(const void* rawData) const noexcept {
    //check if the data is valid
    if (!rawData) {
        return "";
    }

    //Todo: if we add char support, insert char treatment here
    const auto* const pointer = static_cast<char const*>(rawData);
    std::stringstream str;
    str << "[";
    //todo: think about how you can make this prettier
    for (size_t i = 0; i < totalSize; ++i) {
        auto const fieldOffset = physicalComponentType->size();
        const auto* const componentValue = &pointer[fieldOffset * i];
        str << physicalComponentType->convertRawToString(componentValue);
        str << ", ";
    }
    str << "]";
    return str.str();
}

std::string TensorPhysicalType::convertRawToStringWithoutFill(const void* rawData) const noexcept {
    //check if the pointer is valid
    if (!rawData) {
        return "";
    }

    //Todo: if we add char support, insert char treatment here
    const auto* const pointer = static_cast<char const*>(rawData);
    std::stringstream str;
    str << "[";
    //todo: think about how you can make this prettier
    for (size_t i = 0; i < totalSize; ++i) {
        auto const fieldOffset = physicalComponentType->size();
        const auto* const componentValue = &pointer[fieldOffset * i];
        str << physicalComponentType->convertRawToString(componentValue);
        str << ", ";
    }
    str << "]";
    return str.str();
}

}// namespace NES