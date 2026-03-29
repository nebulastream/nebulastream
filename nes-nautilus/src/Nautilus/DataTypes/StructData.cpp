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

#include <Nautilus/DataTypes/StructData.hpp>

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/ostream.h>
#include <nautilus/val.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

StructData::StructData(const nautilus::val<int8_t*>& reference, std::vector<std::pair<std::string, DataType>> fields)
    : ptr(reference), fields(std::move(fields))
{
}

size_t StructData::getNumFields() const
{
    return fields.size();
}

const std::vector<std::pair<std::string, DataType>>& StructData::getFields() const
{
    return fields;
}

nautilus::val<int8_t*> StructData::getRawPtr() const
{
    return ptr;
}

size_t StructData::fieldSizeInBytes(const DataType& fieldType)
{
    switch (fieldType.type)
    {
        case DataType::Type::FIXEDSIZED: {
            const auto elementSize = DataType{fieldType.elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
            return static_cast<size_t>(fieldType.count) * elementSize;
        }
        case DataType::Type::STRUCT: {
            size_t total = 0;
            for (const auto& [name, sub] : fieldType.fields)
            {
                total += fieldSizeInBytes(sub);
            }
            return total;
        }
        default:
            return DataType{fieldType.type, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
    }
}

size_t StructData::getTotalSizeInBytes() const
{
    size_t total = 0;
    for (const auto& [name, fieldType] : fields)
    {
        total += fieldSizeInBytes(fieldType);
    }
    return total;
}

VarVal StructData::at(const size_t fieldIndex) const
{
    if (fieldIndex >= fields.size())
    {
        throw OutOfRangeAccess("StructData::at: field index out of range");
    }
    size_t offset = 0;
    for (size_t i = 0; i < fieldIndex; ++i)
    {
        offset += fieldSizeInBytes(fields[i].second);
    }
    const auto& fieldType = fields[fieldIndex].second;
    const auto fieldPtr = ptr + nautilus::val<size_t>(offset);
    if (fieldType.type == DataType::Type::FIXEDSIZED)
    {
        return VarVal{FixedSizedData{fieldPtr, static_cast<size_t>(fieldType.count), fieldType.elementType}};
    }
    if (fieldType.type == DataType::Type::STRUCT)
    {
        return VarVal{StructData{fieldPtr, fieldType.fields}};
    }
    return VarVal::readNonNullableVarValFromMemory(fieldPtr, DataType{fieldType.type, DataType::NULLABLE::NOT_NULLABLE});
}

VarVal StructData::at(const std::string_view fieldName) const
{
    for (size_t i = 0; i < fields.size(); ++i)
    {
        if (fields[i].first == fieldName)
        {
            return at(i);
        }
    }
    throw FieldNotFound("StructData::at: no field named '{}'", std::string{fieldName});
}

void StructData::writeAt(const size_t fieldIndex, const VarVal& value) const
{
    if (fieldIndex >= fields.size())
    {
        throw OutOfRangeAccess("StructData::writeAt: field index out of range");
    }
    size_t offset = 0;
    for (size_t i = 0; i < fieldIndex; ++i)
    {
        offset += fieldSizeInBytes(fields[i].second);
    }
    const auto& fieldType = fields[fieldIndex].second;
    const auto fieldPtr = ptr + nautilus::val<size_t>(offset);

    if (fieldType.type == DataType::Type::FIXEDSIZED)
    {
        /// VarVal::writeToMemory rejects FixedSizedData, so copy the inline
        /// bytes directly. Width is host-side, so it folds at trace time.
        const auto src = value.getRawValueAs<FixedSizedData>();
        const auto bytes = nautilus::val<uint64_t>(fieldSizeInBytes(fieldType));
        nautilus::memcpy(fieldPtr, src.getRawPtr(), bytes);
        return;
    }
    if (fieldType.type == DataType::Type::STRUCT)
    {
        const auto src = value.getRawValueAs<StructData>();
        const auto bytes = nautilus::val<uint64_t>(fieldSizeInBytes(fieldType));
        nautilus::memcpy(fieldPtr, src.getRawPtr(), bytes);
        return;
    }
    value.writeToMemory(fieldPtr);
}

void StructData::writeAt(const std::string_view fieldName, const VarVal& value) const
{
    for (size_t i = 0; i < fields.size(); ++i)
    {
        if (fields[i].first == fieldName)
        {
            writeAt(i, value);
            return;
        }
    }
    throw FieldNotFound("StructData::writeAt: no field named '{}'", std::string{fieldName});
}

nautilus::val<bool> StructData::operator==(const StructData& rhs) const
{
    if (fields != rhs.fields)
    {
        return {false};
    }
    const auto totalBytes = nautilus::val<uint64_t>(getTotalSizeInBytes());
    return nautilus::memcmp(ptr, rhs.ptr, totalBytes) == 0;
}

nautilus::val<bool> StructData::operator!=(const StructData& rhs) const
{
    return !(*this == rhs);
}

nautilus::val<std::ostream>& operator<<(nautilus::val<std::ostream>& oss, const StructData& structData)
{
    const auto totalBytes = structData.getTotalSizeInBytes();
    oss << "StructData(fields=" << nautilus::val<uint64_t>(structData.fields.size()) << ", bytes=" << nautilus::val<uint64_t>(totalBytes)
        << "): ";
    for (nautilus::val<uint64_t> i = 0; i < nautilus::val<uint64_t>(totalBytes); ++i)
    {
        const nautilus::val<int> byte = readValueFromMemRef<int8_t>(structData.ptr + i) & nautilus::val<int>(0xff);
        oss << nautilus::hex;
        oss.operator<<(byte);
        oss << " ";
    }
    return oss;
}

}
