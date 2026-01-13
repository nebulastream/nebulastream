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
#include <ErrorHandling.hpp>

namespace NES
{

VariableSizedData::VariableSizedData(const nautilus::val<int8_t*>& reference, const nautilus::val<uint64_t>& size)
    : size(size), ptrToVarSized(reference)
{
}

VariableSizedData::VariableSizedData(const VariableSizedData& other)
    : size(other.size)
    , ptrToVarSized(other.ptrToVarSized)
    , arenaRef(other.arenaRef)
    , firstText(other.firstText)
    , secondText(other.secondText)
    , firstSize(other.firstSize)
    , secondSize(other.secondSize)
    , type(other.type)
{
}

VariableSizedData::VariableSizedData(const VariableSizedData& first, const VariableSizedData& second, ArenaRef& arena)
    : size(first.getSize() + second.getSize())
    , arenaRef(&arena)
    , firstText(first.getContent())
    , secondText(second.getContent())
    , firstSize(first.getSize())
    , secondSize(second.getSize())
    , type(VariableSizedType::COMPOUND)
{
}

VariableSizedData& VariableSizedData::operator=(const VariableSizedData& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = other.size;
    ptrToVarSized = other.ptrToVarSized;
    arenaRef = other.arenaRef;
    firstText = other.firstText;
    secondText = other.secondText;
    firstSize = other.firstSize;
    secondSize = other.secondSize;
    type = other.type;

    return *this;
}

VariableSizedData::VariableSizedData(VariableSizedData&& other) noexcept
    : size(std::move(other.size))
    , ptrToVarSized(std::move(other.ptrToVarSized))
    , arenaRef(other.arenaRef)
    , firstText(other.firstText)
    , secondText(other.secondText)
    , firstSize(other.firstSize)
    , secondSize(other.secondSize)
    , type(other.type)
{
}

VariableSizedData& VariableSizedData::operator=(VariableSizedData&& other) noexcept
{
    if (this == &other)
    {
        return *this;
    }

    size = std::move(other.size);
    ptrToVarSized = std::move(other.ptrToVarSized);
    arenaRef = other.arenaRef;
    firstText = std::move(other.firstText);
    secondText = std::move(other.secondText);
    firstSize = std::move(other.firstSize);
    secondSize = std::move(other.secondSize);
    type = other.type;

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
    PRECONDITION(size > 0 && ptrToVarSized != nullptr, "VariableSizedData has a size of 0 but  a nullptr pointer to the data.");
    PRECONDITION(size == 0 && ptrToVarSized == nullptr, "VariableSizedData has a size of 0 so there should be no pointer to the data.");
    return size > 0 && ptrToVarSized != nullptr;
}

nautilus::val<bool> VariableSizedData::operator==(const VariableSizedData& rhs) const
{
    if (size != rhs.size)
    {
        return {false};
    }

    const auto rhsVarSizedData = rhs.getContent();
    if (type == VariableSizedType::REGULAR)
    {
        const auto varSizedData = getContent();
        const auto compareResult = nautilus::memcmp(varSizedData, rhsVarSizedData, size) == 0;
        return {compareResult};
    }
    const auto first = firstText;
    const auto second = secondText;
    const auto compareResult1 = (nautilus::memcmp(rhsVarSizedData, first.value(), firstSize.value()) == 0);
    const auto compareResult2 = (nautilus::memcmp(rhsVarSizedData + firstSize.value(), second.value(), secondSize.value()) == 0);
    return {compareResult1 && compareResult2};
}

void VariableSizedData::materialize() const
{
    ptrToVarSized = arenaRef->allocateMemory(size);

    /// Writing the left value and then the right value to the new variable sized data
    nautilus::memcpy(ptrToVarSized, firstText.value(), firstSize.value());
    nautilus::memcpy(ptrToVarSized + firstSize.value(), secondText.value(), secondSize.value());
}

nautilus::val<bool> VariableSizedData::operator!=(const VariableSizedData& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<bool> VariableSizedData::operator!() const
{
    return !isValid();
}

[[nodiscard]] nautilus::val<uint32_t> VariableSizedData::getSize() const
{
    return size;
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getContent() const
{
    if (type == VariableSizedType::COMPOUND)
    {
        materialize();
    }
    return ptrToVarSized;
}

[[nodiscard]] nautilus::val<int8_t*> VariableSizedData::getReference() const
{
    return ptrToVarSized;
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
