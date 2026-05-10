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

#include <Nautilus/DataTypes/FixedSizedData.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

FixedSizedData::FixedSizedData(const nautilus::val<int8_t*>& reference, const size_t numElements, const DataType::Type elementType)
    : ptr(reference), numElements(numElements), elementType(elementType)
{
}

size_t FixedSizedData::getNumElements() const
{
    return numElements;
}

DataType::Type FixedSizedData::getElementType() const
{
    return elementType;
}

size_t FixedSizedData::getTotalSizeInBytes() const
{
    return numElements * DataType{elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
}

nautilus::val<int8_t*> FixedSizedData::getRawPtr() const
{
    return ptr;
}

VarVal FixedSizedData::at(const nautilus::val<uint64_t>& index) const
{
    #ifndef NDEBUG
    if (index >= nautilus::val<uint64_t>(numElements))
    {
        nautilus::invoke(+[] { throw OutOfRangeAccess("FixedSizedData::at: index out of range"); });
    }
    #endif

    const auto elementSize = DataType{elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
    const auto elementPtr = ptr + (index * nautilus::val<uint64_t>(elementSize));
    return VarVal::readNonNullableVarValFromMemory(elementPtr, DataType{elementType, DataType::NULLABLE::NOT_NULLABLE});
}

void FixedSizedData::writeAt(const nautilus::val<uint64_t>& index, const VarVal& value) const
{
    #ifndef NDEBUG
    if (index >= nautilus::val<uint64_t>(numElements))
    {
        nautilus::invoke(+[] { throw OutOfRangeAccess("FixedSizedData::writeAt: index out of range"); });
    }
    #endif

    const auto elementSize = DataType{elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
    const auto elementPtr = ptr + (index * nautilus::val<uint64_t>(elementSize));
    value.writeToMemory(elementPtr);
}

nautilus::val<bool> FixedSizedData::operator==(const FixedSizedData& rhs) const
{
    if (elementType != rhs.elementType || numElements != rhs.numElements)
    {
        return {false};
    }
    const auto totalBytes = nautilus::val<uint64_t>(getTotalSizeInBytes());
    return nautilus::memcmp(ptr, rhs.ptr, totalBytes) == 0;
}

nautilus::val<bool> FixedSizedData::operator!=(const FixedSizedData& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<bool> FixedSizedData::operator!() const
{
    return numElements == 0 && ptr == nullptr;
}

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const FixedSizedData& fixedSizedData)
{
    const auto totalBytes = fixedSizedData.getTotalSizeInBytes();
    oss << "FixedSizedData(elements=" << nautilus::val<uint64_t>(fixedSizedData.numElements)
        << ", bytes=" << nautilus::val<uint64_t>(totalBytes) << "): ";
    for (nautilus::val<uint64_t> i = 0; i < nautilus::val<uint64_t>(totalBytes); ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>(fixedSizedData.ptr + i) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}

}
