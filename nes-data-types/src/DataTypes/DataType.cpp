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
    const auto isNullable = left.isNullable or right.isNullable;

    /// If left is a float, the result is a float or double depending on the bits of the left float
    if (left.isFloat() and right.isInteger())
    {
        return (left.getRawSizeInBytes() == sizeOfIntInBytes)
            ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32, isNullable)
            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64, isNullable);
    }

    if (left.isInteger() and right.isFloat())
    {
        return (right.getRawSizeInBytes() == sizeOfIntInBytes)
            ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32, isNullable)
            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64, isNullable);
    }

    if (right.isFloat() && left.isFloat())
    {
        return (left.getRawSizeInBytes() == sizeOfLongInBytes or right.getRawSizeInBytes() == sizeOfLongInBytes)
            ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64, isNullable)
            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32, isNullable);
    }

    if (right.isInteger() and left.isInteger())
    {
        /// We need to still cast here to an integer, as the lowerBound is a member of Integer and not of Numeric
        if (left.getRawSizeInBytes() < sizeOfIntInBytes and right.getRawSizeInBytes() < sizeOfIntInBytes)
        {
            return NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable);
        }

        if (left.getRawSizeInBytes() == sizeOfIntInBytes and right.getRawSizeInBytes() < sizeOfIntInBytes)
        {
            return (
                left.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable)
                                       : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32, isNullable));
        }

        if (left.getRawSizeInBytes() < sizeOfIntInBytes and right.getRawSizeInBytes() == sizeOfIntInBytes)
        {
            return (
                right.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable)
                                        : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32, isNullable));
        }

        if (left.getRawSizeInBytes() == sizeOfIntInBytes and right.getRawSizeInBytes() == sizeOfIntInBytes)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger())
                    ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32, isNullable)
                    : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32, isNullable));
        }

        if (left.getRawSizeInBytes() == sizeOfLongInBytes and right.getRawSizeInBytes() < sizeOfLongInBytes)
        {
            return (
                left.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64, isNullable)
                                       : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64, isNullable));
        }

        if (left.getRawSizeInBytes() < sizeOfLongInBytes and right.getRawSizeInBytes() == sizeOfLongInBytes)
        {
            return (
                right.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64, isNullable)
                                        : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64, isNullable));
        }

        if (left.getRawSizeInBytes() == sizeOfLongInBytes and right.getRawSizeInBytes() == sizeOfLongInBytes)
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

/// NOLINTBEGIN(readability-magic-numbers)
uint32_t DataType::getRawSizeInBytes() const
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
            /// Returning '8' for VARSIZED, because we store 'uint64_t' data that represent how to access the data, c.f., @class VariableSizedAccess
            return 8;
        case Type::VARSIZED_POINTER_REP:
            return sizeof(int8_t*);
        case Type::INT64:
        case Type::UINT64:
        case Type::FLOAT64:
            return 8;
        case Type::UNDEFINED:
            return 0;
    }
    std::unreachable();
}

uint32_t DataType::getSizeInBytes() const
{
    const auto extraByte = isNullable ? 1 : 0;
    switch (this->type)
    {
        case Type::INT8:
        case Type::UINT8:
        case Type::BOOLEAN:
        case Type::CHAR:
            return 1 + extraByte;
        case Type::INT16:
        case Type::UINT16:
            return 2 + extraByte;
        case Type::INT32:
        case Type::UINT32:
        case Type::FLOAT32:
            return 4 + extraByte;
        case Type::VARSIZED:
            /// Returning '8' for VARSIZED, because we store 'uint64_t' data that represent how to access the data, c.f., @class VariableSizedAccess
            return 8 + extraByte;
        case Type::VARSIZED_POINTER_REP:
            return sizeof(int8_t*) + extraByte;
        case Type::INT64:
        case Type::UINT64:
        case Type::FLOAT64:
            return 8 + extraByte;
        case Type::UNDEFINED:
            return 0 + extraByte;
    }
    std::unreachable();
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
            if (getSizeInBytes() != 1)
            {
                return "invalid char type";
            }
            return std::string{*static_cast<const char*>(data)};
        }
        case Type::VARSIZED_POINTER_REP:
        case Type::VARSIZED: {
            /// Read the length of the VariableSizedDataType from the first StringLengthType bytes from the buffer and adjust the data pointer.
            using StringLengthType = uint32_t;
            const StringLengthType textLength = *static_cast<const uint32_t*>(data);
            const auto* textPointer = static_cast<const char*>(data);
            textPointer += sizeof(StringLengthType); ///NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            return {textPointer, textLength};
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
    return DataType{.type = DataType::Type::CHAR, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterBOOLEANDataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::BOOLEAN, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT32DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::FLOAT32, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT64DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::FLOAT64, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::INT8, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::INT16, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::INT32, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::INT64, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::UINT8, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::UINT16, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::UINT32, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::UINT64, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUNDEFINEDDataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::UNDEFINED, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZEDDataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::VARSIZED, .isNullable = args.isNullable};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZED_POINTER_REPDataType(const DataTypeRegistryArguments args)
{
    return DataType{.type = DataType::Type::VARSIZED_POINTER_REP, .isNullable = args.isNullable};
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

std::optional<DataType> DataType::join(const DataType& otherDataType) const
{
    const auto isNullable = this->isNullable or otherDataType.isNullable;
    if (this->type == Type::UNDEFINED)
    {
        return {DataTypeProvider::provideDataType(Type::UNDEFINED, isNullable)};
    }
    if (this->type == Type::VARSIZED)
    {
        return (otherDataType.isType(Type::VARSIZED)) ? std::optional{DataTypeProvider::provideDataType(Type::VARSIZED, isNullable)}
                                                      : std::nullopt;
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
            return {DataTypeProvider::provideDataType(Type::CHAR, isNullable)};
        }
        return {DataTypeProvider::provideDataType(Type::UNDEFINED, isNullable)};
    }
    if (this->type == Type::BOOLEAN)
    {
        if (otherDataType.type == Type::BOOLEAN)
        {
            return {DataTypeProvider::provideDataType(Type::BOOLEAN, isNullable)};
        }
        return {DataTypeProvider::provideDataType(Type::UNDEFINED, isNullable)};
    }
    NES_WARNING("Cannot join {} and {}", *this, otherDataType);
    return std::nullopt;
}

std::ostream& operator<<(std::ostream& os, const DataType& dataType)
{
    return os << fmt::format("DataType(type: {} nullable: {})", magic_enum::enum_name(dataType.type), dataType.isNullable);
}

}
