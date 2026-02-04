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

#include <Nautilus/DataTypes/VariableSizedData.hpp>

#include <cstdint>
#include <ostream>
#include <utility>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <Arena.hpp>

namespace NES
{

namespace detail
{

RegularContent::RegularContent(const nautilus::val<int8_t*>& content, nautilus::val<uint64_t> size)
    : ptr(content), contentSize(std::move(size))
{
}

nautilus::val<bool> RegularContent::operator==(const RegularContent& rhs) const
{
    if (contentSize != rhs.contentSize)
    {
        return false;
    }
    return nautilus::memcmp(ptr, rhs.ptr, contentSize) == 0;
}

nautilus::val<bool> RegularContent::operator==(const CompoundContent& rhs) const
{
    // Use operator definition in CompoundContent
    return rhs == *this;
}

CompoundContent::CompoundContent(
    ArenaRef* arena,
    const nautilus::val<int8_t*>& first,
    nautilus::val<uint64_t> firstSize,
    const nautilus::val<int8_t*>& second,
    nautilus::val<uint64_t> secondSize)
    : arenaRef(arena), firstPtr(first), secondPtr(second), firstSize(std::move(firstSize)), secondSize(std::move(secondSize))
{
}

nautilus::val<int8_t*> CompoundContent::getContent() const
{
    if (materializedPtr == nullptr)
    {
        auto ptr = arenaRef->allocateMemory(firstSize + secondSize);
        nautilus::memcpy(ptr, firstPtr, firstSize);
        nautilus::memcpy(ptr + firstSize, secondPtr, secondSize);
        materializedPtr = ptr;
    }
    return materializedPtr;
}

nautilus::val<bool> CompoundContent::operator==(const RegularContent& rhs) const
{
    if (firstPtr + secondSize != rhs.contentSize)
    {
        return false;
    }
    if (const auto compareFirst = nautilus::memcmp(rhs.ptr, firstPtr, firstSize) == 0)
    {
        const auto compareSecond = nautilus::memcmp(rhs.ptr + firstSize, secondPtr, secondSize) == 0;
        return compareSecond;
    }
    return false;
}

nautilus::val<bool> CompoundContent::operator==(const CompoundContent& rhs) const
{
    if (firstSize + secondSize != rhs.firstSize + rhs.secondSize)
    {
        return false;
    }

    if (const auto compareFirstPart = nautilus::memcmp(firstPtr, rhs.firstPtr, firstSize) == 0)
    {
        const auto compareSecondPart = nautilus::memcmp(secondPtr, rhs.secondPtr, secondSize) == 0;
        return compareSecondPart;
    }
    return false;
}

} // namespace detail

VariableSizedData::VariableSizedData(const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size)
    : size(size), content(detail::RegularContent(reference, size))
{
}

VariableSizedData::VariableSizedData(const VariableSizedData& first, const VariableSizedData& second, ArenaRef& arena)
    : size(first.getSize() + second.getSize())
    , content(detail::CompoundContent(&arena, first.getContent(), first.getSize(), second.getContent(), second.getSize()))
{
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
    return std::visit([](const auto& content) { return content.isValid(); }, content);
}

nautilus::val<bool> VariableSizedData::operator==(const VariableSizedData& rhs) const
{
    if (size != rhs.size)
    {
        return {false};
    }

    return std::visit([](const auto& lhs, const auto& rhs) { return lhs == rhs; }, content, rhs.content);
}

nautilus::val<bool> VariableSizedData::operator!=(const VariableSizedData& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<bool> VariableSizedData::operator!() const
{
    return !isValid();
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getContent() const
{
    return std::visit([](const auto& content) { return content.getContent(); }, content);
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getReference() const
{
    return getContent();
}

[[nodiscard]] nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VariableSizedData& variableSizedData)
{
    oss << "Size(" << variableSizedData.size << "): ";
    for (nautilus::val<uint32_t> i = 0; i < variableSizedData.size; ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>((variableSizedData.getContent() + i)) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}

}
