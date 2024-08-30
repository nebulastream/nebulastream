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
#include <iomanip>

namespace NES::Nautilus
{

VariableSizedData::VariableSizedData(const nautilus::val<int8_t*>& content, const nautilus::val<uint32_t>& size)
    : size(size), content(content)
{
}

VariableSizedData::VariableSizedData(const nautilus::val<int8_t*>& pointerToVarSizedData)
    : VariableSizedData(pointerToVarSizedData, readValueFromMemRef(pointerToVarSizedData, uint32_t))
{
}

VariableSizedData::VariableSizedData(const VariableSizedData& other) : size(other.size), content(other.content)
{
}

VariableSizedData& VariableSizedData::operator=(const VariableSizedData& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = other.size;
    content = other.content;
    return *this;
}

VariableSizedData::VariableSizedData(VariableSizedData&& other) noexcept : size(std::move(other.size)), content(std::move(other.content))
{
}

VariableSizedData& VariableSizedData::operator=(VariableSizedData&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = std::move(other.size);
    content = std::move(other.content);
    return *this;
}

nautilus::val<bool> operator==(const VariableSizedData& varSizedData, const nautilus::val<bool>& other)
{
    return varSizedData.isValid() == other;
}

nautilus::val<bool> operator==(const nautilus::val<bool>& other, const VariableSizedData& varSizedData)
{
    return varSizedData.isValid() == other;
}

nautilus::val<bool> VariableSizedData::isValid() const
{
    return size > 0 && content != nullptr;
}

nautilus::val<bool> VariableSizedData::operator==(const VariableSizedData& rhs) const
{
    if (size != rhs.size)
    {
        return {false};
    }

    const auto compareResult = (nautilus::memcmp(content, rhs.content, size) == 0);
    return {compareResult};
}

nautilus::val<bool> VariableSizedData::operator!=(const VariableSizedData& rhs) const
{
    return !(*this == rhs);
}

[[nodiscard]] nautilus::val<uint32_t> VariableSizedData::getSize() const
{
    return size;
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getContent() const
{
    return content + nautilus::val<uint64_t>(sizeof(uint32_t));
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getReference() const
{
    return content;
}

[[nodiscard]] std::ostream& operator<<(std::ostream& oss, const VariableSizedData& varSizedData)
{
    oss << "Size(" << varSizedData.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < varSizedData.size; ++i)
    {
        oss << std::hex << (readValueFromMemRef((varSizedData.content + i + nautilus::val<uint64_t>(sizeof(uint32_t))), int8_t) & 0xff) << " ";
    }
    return oss;
}
} // namespace NES::Nautilus
