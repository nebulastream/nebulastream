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

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <magic_enum/magic_enum.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

uint32_t DataType::getSizeInBytes() const
{
    return sizeInBits / 8;
}
std::string DataType::formattedBytesToString(const void* data) const
{
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
            return std::to_string(*static_cast<const bool*>(data));
        case Type::CHAR: {
            if (getSizeInBytes() != 1)
            {
                return "invalid char type";
            }
            return std::string{*static_cast<const char*>(data)};
        }
        case Type::VARSIZED: {
            if (!data)
            {
                NES_ERROR("Pointer to variable sized data is invalid. Buffer must at least contain the length (0 if empty).");
                return "";
            }

            /// Read the length of the VariableSizedDataType from the first StringLengthType bytes from the buffer and adjust the data pointer.
            using StringLengthType = uint32_t;
            const StringLengthType textLength = *static_cast<const uint32_t*>(data);
            const auto* textPointer = static_cast<const char*>(data);
            if (textPointer == nullptr)
            {
                NES_ERROR("Pointer to VariableSizedData is invalid.");
                return "";
            }
            textPointer += sizeof(StringLengthType);
            return std::string(textPointer, textLength);
        }
        default:
            return "invalid physical type";
    }
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterCHARDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::CHAR, .sizeInBits = 8};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterBOOLEANDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::BOOLEAN, .sizeInBits = 8};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT32DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::FLOAT32, .sizeInBits = 32};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT64DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::FLOAT64, .sizeInBits = 64};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT8, .sizeInBits = 8};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT16, .sizeInBits = 16};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT32, .sizeInBits = 32};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::INT64, .sizeInBits = 64};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT8, .sizeInBits = 8};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT16, .sizeInBits = 16};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT32, .sizeInBits = 32};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UINT64, .sizeInBits = 64};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUNDEFINEDDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::UNDEFINED, .sizeInBits = 0};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZEDDataType(DataTypeRegistryArguments)
{
    return DataType{.type = DataType::Type::VARSIZED, .sizeInBits = 32};
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
bool DataType::isBoolean() const
{
    return this->type == Type::BOOLEAN;
}
bool DataType::isChar() const
{
    return this->type == Type::CHAR;
}
bool DataType::isVarSized() const
{
    return this->type == Type::VARSIZED;
}
bool DataType::isUndefined() const
{
    return this->type == Type::UNDEFINED;
}

std::optional<DataType> inferNumericDataType(const DataType& left, const DataType& right)
{
    /// We infer the data types between two numerics by following the c++ rules. For example, anything below i32 will be casted to a i32.
    /// Unsigned and signed of the same data type will be casted to the unsigned data type.
    /// For a playground, please take a look at the godbolt link: https://godbolt.org/z/j1cTfczbh
    constexpr int8_t sizeOfIntInBits = sizeof(int32_t) * 8;
    constexpr int8_t sizeOfLongInBits = sizeof(int64_t) * 8;

    /// If left is a float, the result is a float or double depending on the bits of the left float
    if (left.isFloat() and right.isInteger())
    {
        return (left.sizeInBits == sizeOfIntInBits) ? DataTypeProvider::provideDataType(DataType::Type::FLOAT32)
                                                    : DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
    }

    if (left.isInteger() and right.isFloat())
    {
        return (right.sizeInBits == sizeOfIntInBits) ? DataTypeProvider::provideDataType(DataType::Type::FLOAT32)
                                                     : DataTypeProvider::provideDataType(DataType::Type::FLOAT64);
    }

    if (right.isFloat() && left.isFloat())
    {
        return (left.sizeInBits == sizeOfLongInBits or right.sizeInBits == sizeOfLongInBits)
            ? DataTypeProvider::provideDataType(DataType::Type::FLOAT64)
            : DataTypeProvider::provideDataType(DataType::Type::FLOAT32);
    }

    if (right.isInteger() and left.isInteger())
    {
        /// We need to still cast here to an integer, as the lowerBound is a member of Integer and not of Numeric
        const auto leftBits = left.sizeInBits;
        const auto rightBits = right.sizeInBits;

        if (leftBits < sizeOfIntInBits and rightBits < sizeOfIntInBits)
        {
            return DataTypeProvider::provideDataType(DataType::Type::INT32);
        }

        if (leftBits == sizeOfIntInBits and rightBits < sizeOfIntInBits)
        {
            return (
                left.isSignedInteger() ? DataTypeProvider::provideDataType(DataType::Type::INT32)
                                       : DataTypeProvider::provideDataType(DataType::Type::UINT32));
        }

        if (leftBits < sizeOfIntInBits and rightBits == sizeOfIntInBits)
        {
            return (
                right.isSignedInteger() ? DataTypeProvider::provideDataType(DataType::Type::INT32)
                                        : DataTypeProvider::provideDataType(DataType::Type::UINT32));
        }

        if (leftBits == sizeOfIntInBits and rightBits == sizeOfIntInBits)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger()) ? DataTypeProvider::provideDataType(DataType::Type::INT32)
                                                                     : DataTypeProvider::provideDataType(DataType::Type::UINT32));
        }

        if (leftBits == sizeOfLongInBits and rightBits < sizeOfLongInBits)
        {
            return (
                left.isSignedInteger() ? DataTypeProvider::provideDataType(DataType::Type::INT64)
                                       : DataTypeProvider::provideDataType(DataType::Type::UINT64));
        }

        if (leftBits < sizeOfLongInBits and rightBits == sizeOfLongInBits)
        {
            return (
                right.isSignedInteger() ? DataTypeProvider::provideDataType(DataType::Type::INT64)
                                        : DataTypeProvider::provideDataType(DataType::Type::UINT64));
        }

        if (leftBits == sizeOfLongInBits and rightBits == sizeOfLongInBits)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger()) ? DataTypeProvider::provideDataType(DataType::Type::INT64)
                                                                     : DataTypeProvider::provideDataType(DataType::Type::UINT64));
        }
    }

    return {};
}

DataType DataType::join(const DataType& otherDataType)
{
    if (this->type == Type::UNDEFINED or this->type == Type::VARSIZED)
    {
        return DataTypeProvider::provideDataType(Type::UNDEFINED);
    }

    if (this->isNumeric())
    {
        if (otherDataType.type == Type::UNDEFINED)
        {
            return DataType{};
        }

        if (not otherDataType.isNumeric())
        {
            throw DifferentFieldTypeExpected("Cannot join {} and {}", "*this", otherDataType);
        }

        if (const auto newDataType = inferNumericDataType(*this, otherDataType); newDataType.has_value())
        {
            return newDataType.value();
        }
    }
    if (this->type == Type::CHAR)
    {
        if (otherDataType.type == Type::CHAR)
        {
            return DataTypeProvider::provideDataType(Type::CHAR);
        }
        return DataTypeProvider::provideDataType(Type::UNDEFINED);
    }
    if (this->type == Type::BOOLEAN)
    {
        if (otherDataType.type == Type::BOOLEAN)
        {
            return DataTypeProvider::provideDataType(Type::BOOLEAN);
        }
        return DataTypeProvider::provideDataType(Type::UNDEFINED);
    }
    throw DifferentFieldTypeExpected("Cannot join {} and {}", "*this", otherDataType);
}
std::ostream& operator<<(std::ostream& os, const DataType& dataType)
{
    return os << fmt::format("DataType(type: {}, sizeInBits: {})", magic_enum::enum_name(dataType.type), dataType.sizeInBits);
}
}
