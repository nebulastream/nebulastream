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

#include <Nautilus/DataTypes/VarArrayData.hpp>

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
VarArrayData::VarArrayData(const nautilus::val<int8_t*>& reference, DataType::Type elementType, const nautilus::val<uint64_t>& size)
    : ptr(reference), size(size), elementType(elementType)
{
    /// For now, we forbid nullable elements
    numElements = nautilus::val<uint64_t>{size / DataType{elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull()};
}

DataType::Type VarArrayData::getElementType() const
{
    return elementType;
}

nautilus::val<uint64_t> VarArrayData::getNumElements() const
{
    return numElements;
}

nautilus::val<int8_t*> VarArrayData::getRawPtr() const
{
    return ptr;
}

nautilus::val<uint64_t> VarArrayData::getTotalSizeInBytes() const
{
    return size;
}

VarVal VarArrayData::at(const nautilus::val<uint64_t>& index)
{
#ifndef NDEBUG
    if (index >= nautilus::val<uint64_t>(numElements))
    {
        nautilus::invoke(+[] { throw OutOfRangeAccess("VarArrayData::at: index out of range"); });
    }
#endif

    const auto elementSize = DataType{elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
    const auto elementPtr = ptr + (index * nautilus::val<uint64_t>{elementSize});
    return VarVal::readNonNullableVarValFromMemory(elementPtr, DataType{elementType, DataType::NULLABLE::NOT_NULLABLE});
}

void VarArrayData::writeAt(const nautilus::val<uint64_t>& index, const VarVal& value)
{
#ifndef NDEBUG
    if (index >= nautilus::val<uint64_t>(numElements))
    {
        nautilus::invoke(+[] { throw OutOfRangeAccess("VarArrayData::writeAt: index out of range"); });
    }
#endif
    const auto elementSize = DataType{elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
    const auto elementPtr = ptr + (index * nautilus::val<uint64_t>{elementSize});
    value.writeToMemory(elementPtr);
}

nautilus::val<bool> VarArrayData::operator==(const VarArrayData& rhs) const
{
    if (elementType != rhs.elementType || numElements != rhs.numElements)
    {
        return {false};
    }
    return nautilus::memcmp(ptr, rhs.ptr, size) == 0;
}

nautilus::val<bool> VarArrayData::operator!() const
{
    return numElements == 0 && ptr == nullptr;
}

nautilus::val<bool> VarArrayData::operator!=(const VarArrayData& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const VarArrayData& varSizedData)
{
    oss << "VarArrayData(elements=" << varSizedData.numElements << ", bytes=" << varSizedData.size << "): ";
    for (nautilus::val<uint64_t> i = 0; i < varSizedData.size; ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>(varSizedData.ptr + i) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}
}
