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

#include <memory>
#include <string>
#include <Util/Logger/Logger.hpp>
#include <fmt/core.h>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES
{

uint64_t VariableSizedDataPhysicalType::size() const
{
    /// returning the size of the index to the child buffer that contains the VariableSizedData data
    return sizeVal;
}

std::string VariableSizedDataPhysicalType::convertRawToString(const void* data) const noexcept
{
    /// We always read the exact number of bytes contained by the VariableSizedDataType.
    return convertRawToStringWithoutFill(data);
}

std::unique_ptr<PhysicalType> VariableSizedDataPhysicalType::clone() const
{
    return std::make_unique<VariableSizedDataPhysicalType>(*this);
}

std::string VariableSizedDataPhysicalType::convertRawToStringWithoutFill(const void* data) const noexcept
{
    if (!data)
    {
        NES_ERROR("Pointer to variable sized data is invalid. Buffer must at least contain the length (0 if empty).");
        return "";
    }

    /// Read the length of the VariableSizedDataType from the first StringLengthType bytes from the buffer and adjust the data pointer.
    using StringLengthType = uint32_t;
    const StringLengthType textLength = *static_cast<const uint32_t*>(data);
    const auto* textPointer = static_cast<const char*>(data);
    textPointer += sizeof(StringLengthType);
    if (!textPointer)
    {
        NES_ERROR("Pointer to VariableSizedData is invalid.");
        return "";
    }
    return std::string(textPointer, textLength);
}

std::string VariableSizedDataPhysicalType::toString() const noexcept
{
    return "VariableSizedData " + std::to_string(sizeVal);
}

}
