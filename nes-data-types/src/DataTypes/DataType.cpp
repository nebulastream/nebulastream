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
#include <DataTypes/DataType.hpp>

#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <utility>

#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace
{

std::optional<NES::DataType> inferNumericDataType(const NES::DataType& left, const NES::DataType& right)
{
    /// We infer the data types between two numerics by following the c++ rules. For example, anything below i32 will be casted to a i32.
    /// Unsigned and signed of the same data type will be casted to the unsigned data type.
    /// For a playground, please take a look at the godbolt link: https://godbolt.org/z/j1cTfczbh
    constexpr int8_t sizeOfIntInBytes = sizeof(int32_t);
    constexpr int8_t sizeOfLongInBytes = sizeof(int64_t);
    const auto isNullable = left.nullable or right.nullable ? NES::DataType::NULLABLE::IS_NULLABLE : NES::DataType::NULLABLE::NOT_NULLABLE;

    /// If left is a float, the result is a float or double depending on the bits of the left float
    if (left.isFloat() and right.isInteger())
    {
        return (left.getSizeInBytesWithoutNull() == sizeOfIntInBytes)
            ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32, isNullable)
            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64, isNullable);
    }

    if (left.isInteger() and right.isFloat())
    {
        return (right.getSizeInBytesWithoutNull() == sizeOfIntInBytes)
            ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32, isNullable)
            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64, isNullable);
    }

    if (right.isFloat() && left.isFloat())
    {
        return (left.getSizeInBytesWithoutNull() == sizeOfLongInBytes or right.getSizeInBytesWithoutNull() == sizeOfLongInBytes)
            ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64, isNullable)
            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32, isNullable);
    }

    if (right.isInteger() and left.isInteger())
    {
        /// We need to still cast here to an integer, as the lowerBound is a member of Integer and not of Numeric
        if (left.getSizeInBytesWithoutNull() < sizeOfIntInBytes and right.getSizeInBytesWithoutNull() < sizeOfIntInBytes)
        {
            return NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable);
        }

        if (left.getSizeInBytesWithoutNull() == sizeOfIntInBytes and right.getSizeInBytesWithoutNull() < sizeOfIntInBytes)
        {
            return (
                left.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable)
                                       : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32, isNullable));
        }

        if (left.getSizeInBytesWithoutNull() < sizeOfIntInBytes and right.getSizeInBytesWithoutNull() == sizeOfIntInBytes)
        {
            return (
                right.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable)
                                        : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32, isNullable));
        }

        if (left.getSizeInBytesWithoutNull() == sizeOfIntInBytes and right.getSizeInBytesWithoutNull() == sizeOfIntInBytes)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger())
                    ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable)
                    : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32, isNullable));
        }

        if (left.getSizeInBytesWithoutNull() == sizeOfLongInBytes and right.getSizeInBytesWithoutNull() < sizeOfLongInBytes)
        {
            return (
                left.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64, isNullable)
                                       : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64, isNullable));
        }

        if (left.getSizeInBytesWithoutNull() < sizeOfLongInBytes and right.getSizeInBytesWithoutNull() == sizeOfLongInBytes)
        {
            return (
                right.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64, isNullable)
                                        : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64, isNullable));
        }

        if (left.getSizeInBytesWithoutNull() == sizeOfLongInBytes and right.getSizeInBytesWithoutNull() == sizeOfLongInBytes)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger())
                    ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64, isNullable)
                    : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64, isNullable));
        }
    }

    return {};
}
}

namespace NES
{

DataType::DataType(const Type type, const NULLABLE nullable) : type(type), nullable(nullable == NULLABLE::IS_NULLABLE)
{
}

DataType::DataType(const Type type, const NULLABLE nullable, const Type elementType, const uint32_t count)
    : type(type), nullable(nullable == NULLABLE::IS_NULLABLE), elementType(elementType), count(count)
{
    if (type != Type::FIXEDSIZED)
    {
        throw DifferentFieldTypeExpected(
            "The elementType/count DataType constructor is fixed-sized only, but got: {}", magic_enum::enum_name(type));
    }
}

DataType::DataType(
    const Type type, const NULLABLE nullable, std::string structName, std::vector<std::pair<std::string, DataType>> fields)
    : type(type), nullable(nullable == NULLABLE::IS_NULLABLE), structName(std::move(structName)), fields(std::move(fields))
{
    if (type != Type::STRUCT)
    {
        throw DifferentFieldTypeExpected(
            "The structName/fields DataType constructor is STRUCT only, but got: {}", magic_enum::enum_name(type));
    }
}

DataType::DataType() : type(Type::UNDEFINED), nullable(true)
{
}

namespace
{
/// Inline byte size of a single field for STRUCT layouts.
///
/// STRUCT bytes are laid out in-place: a primitive uses its native size, a
/// FIXEDSIZED field is `count * elementSize` (no offset/size indirection —
/// indirection is only for top-level FIXEDSIZED in a tuple buffer), and a
/// nested STRUCT recurses with the same rule.
///
/// Mirrors `StructData::fieldSizeInBytes` in nes-nautilus; the two must agree
/// or tuple-buffer reads will misinterpret bytes.
uint32_t inlineFieldSizeInBytes(const NES::DataType& field)
{
    using Type = NES::DataType::Type;
    using NULLABLE = NES::DataType::NULLABLE;
    if (field.type == Type::FIXEDSIZED)
    {
        const auto elementSize = NES::DataType{field.elementType, NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
        return field.count * elementSize;
    }
    if (field.type == Type::STRUCT)
    {
        uint32_t total = 0;
        for (const auto& [name, sub] : field.fields)
        {
            total += inlineFieldSizeInBytes(sub);
        }
        return total;
    }
    return NES::DataType{field.type, NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
}
}

/// NOLINTBEGIN(readability-magic-numbers)
uint32_t DataType::getSizeInBytesWithoutNull() const
{
    switch (this->type)
    {
        case Type::INT8:
        case Type::UINT8:
        case Type::BOOLEAN:
        case Type::CHAR:
            return 1;
        case Type::INT16:
        case Type::UINT16:
            return 2;
        case Type::INT32:
        case Type::UINT32:
        case Type::FLOAT32:
            return 4;
        case Type::VARSIZED:
            /// Returning '16' for VARSIZED, because we store 'uint64_t' 8-byte data that represent how to access the data, c.f., @class VariableSizedAccess
            /// and 8 bytes for the size of the VARSIZED
            return 16;
        case Type::FIXEDSIZED:
            /// We store FIXEDSIZED like VARSZIED for now. After moving physical details, like sizes in bytes, into the memory layout, we
            /// may store FIXEDSIZED in-place
            return 16;
        case Type::STRUCT: {
            /// Inline layout: same rules as `StructData` in nes-nautilus.
            /// Per-field nullability is intentionally ignored for the PoC.
            uint32_t total = 0;
            for (const auto& [name, field] : fields)
            {
                total += inlineFieldSizeInBytes(field);
            }
            return total;
        }
        case Type::INT64:
        case Type::UINT64:
        case Type::FLOAT64:
            return 8;
        case Type::UNDEFINED:
            return 0;
    }
    std::unreachable();
}

uint32_t DataType::getSizeInBytesWithNull() const
{
    return getSizeInBytesWithoutNull() + static_cast<uint32_t>(nullable);
}

/// NOLINTEND(readability-magic-numbers)

std::string DataType::formattedBytesToString(const void* data) const
{
    PRECONDITION(data != nullptr, "Pointer to data is invalid.");
    switch (type)
    {
        case Type::INT8:
            return std::to_string(*static_cast<const int8_t*>(data));
        case Type::UINT8:
            return std::to_string(*static_cast<const uint8_t*>(data));
        case Type::INT16:
            return std::to_string(*static_cast<const int16_t*>(data));
        case Type::UINT16:
            return std::to_string(*static_cast<const uint16_t*>(data));
        case Type::INT32:
            return std::to_string(*static_cast<const int32_t*>(data));
        case Type::UINT32:
            return std::to_string(*static_cast<const uint32_t*>(data));
        case Type::INT64:
            return std::to_string(*static_cast<const int64_t*>(data));
        case Type::UINT64:
            return std::to_string(*static_cast<const uint64_t*>(data));
        case Type::FLOAT32:
            return formatFloat(*static_cast<const float*>(data));
        case Type::FLOAT64:
            return formatFloat(*static_cast<const double*>(data));
        case Type::BOOLEAN:
            return std::to_string(static_cast<int>(*static_cast<const bool*>(data)));
        case Type::CHAR: {
            if (getSizeInBytesWithoutNull() != 1)
            {
                return "invalid char type";
            }
            return std::string{*static_cast<const char*>(data)};
        }
        case Type::VARSIZED: {
            const auto* textPointer = static_cast<const char*>(data);
            return textPointer;
        }
        case Type::FIXEDSIZED: {
            /// Best-effort textual rendering for output formatting; consumers that need
            /// typed access should go through `FixedSizedData::at()` instead.
            const auto elementSize = DataType{elementType, NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
            std::string out = "[";
            const auto* bytes = static_cast<const std::byte*>(data);
            for (uint32_t i = 0; i < count; ++i)
            {
                if (i > 0)
                {
                    out += ",";
                }
                out += DataType{elementType, NULLABLE::NOT_NULLABLE}.formattedBytesToString(bytes + (i * elementSize));
            }
            out += "]";
            return out;
        }
        case Type::STRUCT: {
            /// Walks the inline struct bytes in field order; offsets follow the
            /// same rules as `inlineFieldSizeInBytes` so this stays in sync with
            /// how the bytes were written.
            std::string out = structName + "{";
            const auto* bytes = static_cast<const std::byte*>(data);
            uint32_t offset = 0;
            bool first = true;
            for (const auto& [name, field] : fields)
            {
                if (!first)
                {
                    out += ",";
                }
                first = false;
                out += name + ":" + field.formattedBytesToString(bytes + offset);
                offset += inlineFieldSizeInBytes(field);
            }
            out += "}";
            return out;
        }
        case Type::UNDEFINED:
            return "invalid physical type";
    }
    std::unreachable();
}

bool DataType::isType(const Type type) const
{
    return this->type == type;
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterCHARDataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::CHAR, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterBOOLEANDataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::BOOLEAN, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT32DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::FLOAT32, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT64DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::FLOAT64, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT8, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT16, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT32, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::INT64, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT8, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT16, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT32, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UINT64, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUNDEFINEDDataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::UNDEFINED, args.nullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZEDDataType(const DataTypeRegistryArguments args)
{
    return DataType{DataType::Type::VARSIZED, args.nullable};
}

bool DataType::isInteger() const
{
    return this->type == Type::UINT8 or this->type == Type::UINT16 or this->type == Type::UINT32 or this->type == Type::UINT64
        or this->type == Type::INT8 or this->type == Type::INT16 or this->type == Type::INT32 or this->type == Type::INT64;
}

bool DataType::isSignedInteger() const
{
    return this->type == Type::INT8 or this->type == Type::INT16 or this->type == Type::INT32 or this->type == Type::INT64;
}

bool DataType::isFloat() const
{
    return this->type == Type::FLOAT32 or this->type == Type::FLOAT64;
}

bool DataType::isNumeric() const
{
    return isInteger() or isFloat();
}

DataType::NULLABLE DataType::joinNullable(const DataType& otherDataType) const
{
    const auto isNullableResult
        = this->nullable or otherDataType.nullable ? NES::DataType::NULLABLE::IS_NULLABLE : NES::DataType::NULLABLE::NOT_NULLABLE;
    return isNullableResult;
}

std::optional<DataType> DataType::join(const DataType& otherDataType) const
{
    const auto isNullableResult = joinNullable(otherDataType);
    if (this->type == Type::UNDEFINED)
    {
        return {DataTypeProvider::provideDataType(Type::UNDEFINED, isNullableResult)};
    }
    if (this->type == Type::VARSIZED)
    {
        return (otherDataType.isType(Type::VARSIZED)) ? std::optional{DataTypeProvider::provideDataType(Type::VARSIZED, isNullableResult)}
                                                      : std::nullopt;
    }
    if (this->type == Type::FIXEDSIZED)
    {
        if (otherDataType.type == Type::FIXEDSIZED && otherDataType.elementType == this->elementType && otherDataType.count == this->count)
        {
            return DataType{Type::FIXEDSIZED, isNullableResult, this->elementType, this->count};
        }
        return std::nullopt;
    }
    if (this->type == Type::STRUCT)
    {
        /// Nominal typing: only joinable to a STRUCT with the same name and field layout.
        if (otherDataType.type == Type::STRUCT && otherDataType.structName == this->structName && otherDataType.fields == this->fields)
        {
            return DataType{Type::STRUCT, isNullableResult, this->structName, this->fields};
        }
        return std::nullopt;
    }

    if (this->isNumeric())
    {
        if (otherDataType.type == Type::UNDEFINED)
        {
            return {DataType{}};
        }

        if (not otherDataType.isNumeric())
        {
            NES_WARNING("Cannot join {} and {}", *this, otherDataType);
            return std::nullopt;
        }

        if (const auto newDataType = inferNumericDataType(*this, otherDataType); newDataType.has_value())
        {
            return newDataType;
        }
        NES_WARNING("Cannot join {} and {}", *this, otherDataType);
        return std::nullopt;
    }
    if (this->type == Type::CHAR)
    {
        if (otherDataType.type == Type::CHAR)
        {
            return {DataTypeProvider::provideDataType(Type::CHAR, isNullableResult)};
        }
        return {DataTypeProvider::provideDataType(Type::UNDEFINED, isNullableResult)};
    }
    if (this->type == Type::BOOLEAN)
    {
        if (otherDataType.type == Type::BOOLEAN)
        {
            return {DataTypeProvider::provideDataType(Type::BOOLEAN, isNullableResult)};
        }
        return {DataTypeProvider::provideDataType(Type::UNDEFINED, isNullableResult)};
    }
    NES_WARNING("Cannot join {} and {}", *this, otherDataType);
    return std::nullopt;
}

Reflected Reflector<DataType>::operator()(const DataType& field) const
{
    return reflect(std::make_tuple(field.type, field.nullable, field.elementType, field.count, field.structName, field.fields));
}

DataType Unreflector<DataType>::operator()(const Reflected& rfl) const
{
    using TupleT
        = std::tuple<DataType::Type, bool, DataType::Type, uint32_t, std::string, std::vector<std::pair<std::string, DataType>>>;
    const auto [type, nullable, elementType, count, structName, fields] = unreflect<TupleT>(rfl);
    const auto nullableEnum = nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    if (type == DataType::Type::FIXEDSIZED)
    {
        return DataType{type, nullableEnum, elementType, count};
    }
    if (type == DataType::Type::STRUCT)
    {
        return DataType{type, nullableEnum, structName, fields};
    }
    return DataTypeProvider::provideDataType(type, nullableEnum);
}

std::ostream& operator<<(std::ostream& os, const DataType& dataType)
{
    if (dataType.type == DataType::Type::FIXEDSIZED)
    {
        return os << fmt::format(
                   "DataType(type: FIXEDSIZED<{}, {}> nullable: {})",
                   magic_enum::enum_name(dataType.elementType),
                   dataType.count,
                   dataType.nullable);
    }
    if (dataType.type == DataType::Type::STRUCT)
    {
        std::string fieldList;
        bool first = true;
        for (const auto& [name, field] : dataType.fields)
        {
            if (!first)
            {
                fieldList += ", ";
            }
            first = false;
            fieldList += fmt::format("{}: {}", name, field);
        }
        return os << fmt::format("DataType(type: STRUCT<{}, {{{}}}> nullable: {})", dataType.structName, fieldList, dataType.nullable);
    }
    return os << fmt::format("DataType(type: {} nullable: {})", magic_enum::enum_name(dataType.type), dataType.nullable);
}

}
