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

    /// If left is a float, the result is a float or double depending on the bits of the left float
    if (left.isFloat() and right.isInteger())
    {
        return (left.getSizeInBytes() == sizeOfIntInBytes) ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32)
                                                           : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64);
    }

    if (left.isInteger() and right.isFloat())
    {
        return (right.getSizeInBytes() == sizeOfIntInBytes) ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32)
                                                            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64);
    }

    if (right.isFloat() && left.isFloat())
    {
        return (left.getSizeInBytes() == sizeOfLongInBytes or right.getSizeInBytes() == sizeOfLongInBytes)
            ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT64)
            : NES::DataTypeProvider::provideDataType(NES::DataType::Type::FLOAT32);
    }

    if (right.isInteger() and left.isInteger())
    {
        /// We need to still cast here to an integer, as the lowerBound is a member of Integer and not of Numeric
        if (left.getSizeInBytes() < sizeOfIntInBytes and right.getSizeInBytes() < sizeOfIntInBytes)
        {
            return NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32);
        }

        if (left.getSizeInBytes() == sizeOfIntInBytes and right.getSizeInBytes() < sizeOfIntInBytes)
        {
            return (
                left.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32)
                                       : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32));
        }

        if (left.getSizeInBytes() < sizeOfIntInBytes and right.getSizeInBytes() == sizeOfIntInBytes)
        {
            return (
                right.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32)
                                        : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32));
        }

        if (left.getSizeInBytes() == sizeOfIntInBytes and right.getSizeInBytes() == sizeOfIntInBytes)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger()) ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT32)
                                                                     : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT32));
        }

        if (left.getSizeInBytes() == sizeOfLongInBytes and right.getSizeInBytes() < sizeOfLongInBytes)
        {
            return (
                left.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64)
                                       : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64));
        }

        if (left.getSizeInBytes() < sizeOfLongInBytes and right.getSizeInBytes() == sizeOfLongInBytes)
        {
            return (
                right.isSignedInteger() ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64)
                                        : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64));
        }

        if (left.getSizeInBytes() == sizeOfLongInBytes and right.getSizeInBytes() == sizeOfLongInBytes)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger()) ? NES::DataTypeProvider::provideDataType(NES::DataType::Type::INT64)
                                                                     : NES::DataTypeProvider::provideDataType(NES::DataType::Type::UINT64));
        }
    }

    return {};
}
}

namespace NES
{

/// NOLINTBEGIN(readability-magic-numbers)
uint32_t DataType::getSizeInBytes() const
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
            /// Returning '8' for VARSIZED, because we represent variable sized data with a 'uint64_t' index to a nested buffer, containing the varsized data
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
            return Util::formatFloat(*static_cast<const float*>(data));
        case Type::FLOAT64:
            return Util::formatFloat(*static_cast<const double*>(data));
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

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterCHARDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::CHAR};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterBOOLEANDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::BOOLEAN};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT32DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::FLOAT32};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT64DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::FLOAT64};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT8};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT16};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT32};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT64};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT8};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT16};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT32};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT64};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUNDEFINEDDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UNDEFINED};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZEDDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::VARSIZED};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZED_POINTER_REPDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::VARSIZED_POINTER_REP};
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
    if (this->type == Type::UNDEFINED or this->type == Type::VARSIZED)
    {
        return {DataTypeProvider::provideDataType(Type::UNDEFINED)};
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
            return {DataTypeProvider::provideDataType(Type::CHAR)};
        }
        return {DataTypeProvider::provideDataType(Type::UNDEFINED)};
    }
    if (this->type == Type::BOOLEAN)
    {
        if (otherDataType.type == Type::BOOLEAN)
        {
            return {DataTypeProvider::provideDataType(Type::BOOLEAN)};
        }
        return {DataTypeProvider::provideDataType(Type::UNDEFINED)};
    }
    NES_WARNING("Cannot join {} and {}", *this, otherDataType);
    return std::nullopt;
}

std::ostream& operator<<(std::ostream& os, const DataType& dataType)
{
    return os << fmt::format("DataType(type: {})", magic_enum::enum_name(dataType.type));
}

}
