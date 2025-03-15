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

uint32_t PhysicalType::getSizeInBytes() const
{
    return sizeInBits / 8;
}
std::string PhysicalType::formattedBytesToString(const void* data) const
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
            textPointer += sizeof(StringLengthType);
            if (!textPointer)
            {
                NES_ERROR("Pointer to VariableSizedData is invalid.");
                return "";
            }
            return std::string(textPointer, textLength);
        }
        default:
            return "invalid physical type";
    }
}

std::ostream& operator<<(std::ostream& os, const PhysicalType& physicalType)

{
    return os << fmt::format(
               "PhysicalType(type: {}, sizeInBits: {}, isSigned: {})", magic_enum::enum_name(physicalType.type), physicalType.sizeInBits);
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterCHARDataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::CHAR, .sizeInBits = 8}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterBOOLEANDataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::BOOLEAN, .sizeInBits = 8}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT32DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::FLOAT32, .sizeInBits = 32}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterFLOAT64DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::FLOAT64, .sizeInBits = 64}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT8DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::INT8, .sizeInBits = 8}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT16DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::INT16, .sizeInBits = 16}};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT32DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::INT32, .sizeInBits = 32}};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterINT64DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::INT64, .sizeInBits = 64}};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT8DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::UINT8, .sizeInBits = 8}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT16DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::UINT16, .sizeInBits = 16}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT32DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::UINT32, .sizeInBits = 32}};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUINT64DataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::UINT64, .sizeInBits = 64}};
}


DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterUNDEFINEDDataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::UNDEFINED, .sizeInBits = 0}};
}

DataTypeRegistryReturnType DataTypeGeneratedRegistrar::RegisterVARSIZEDDataType(DataTypeRegistryArguments)
{
    return DataType{PhysicalType{.type = PhysicalType::Type::VARSIZED, .sizeInBits = 32}};
}

bool DataType::isInteger() const
{
    const auto thisType = physicalType.type;
    return thisType == PhysicalType::Type::UINT8 or thisType == PhysicalType::Type::UINT16 or thisType == PhysicalType::Type::UINT32
        or thisType == PhysicalType::Type::UINT64 or thisType == PhysicalType::Type::INT8 or thisType == PhysicalType::Type::INT16
        or thisType == PhysicalType::Type::INT32 or thisType == PhysicalType::Type::INT64;
}
bool DataType::isSignedInteger() const
{
    const auto thisType = physicalType.type;
    return thisType == PhysicalType::Type::INT8 or thisType == PhysicalType::Type::INT16 or thisType == PhysicalType::Type::INT32
        or thisType == PhysicalType::Type::INT64;
}
bool DataType::isFloat() const
{
    return this->physicalType.type == PhysicalType::Type::FLOAT32 or this->physicalType.type == PhysicalType::Type::FLOAT64;
}
bool DataType::isNumeric() const
{
    return isInteger() or isFloat();
}
bool DataType::isBoolean() const
{
    return this->physicalType.type == PhysicalType::Type::BOOLEAN;
}
bool DataType::isChar() const
{
    return this->physicalType.type == PhysicalType::Type::CHAR;
}
bool DataType::isVarSized() const
{
    return this->physicalType.type == PhysicalType::Type::VARSIZED;
}
bool DataType::isUndefined() const
{
    return this->physicalType.type == PhysicalType::Type::UNDEFINED;
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
        return (left.physicalType.sizeInBits == sizeOfIntInBits) ? DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)
                                                                 : DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64);
    }

    if (left.isInteger() and right.isFloat())
    {
        return (right.physicalType.sizeInBits == sizeOfIntInBits) ? DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32)
                                                                  : DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64);
    }


    // if (Util::instanceOf<const Float>(right) && Util::instanceOf<const Float>(left))
    if (right.isFloat() && left.isFloat())
    {
        return (left.physicalType.sizeInBits == sizeOfLongInBits or right.physicalType.sizeInBits == sizeOfLongInBits)
            ? DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT64)
            : DataTypeProvider::provideDataType(PhysicalType::Type::FLOAT32);
    }

    if (right.isInteger() and left.isInteger())
    {
        /// We need to still cast here to an integer, as the lowerBound is a member of Integer and not of Numeric
        const auto leftBits = left.physicalType.sizeInBits;
        const auto rightBits = right.physicalType.sizeInBits;

        if (leftBits < sizeOfIntInBits and rightBits < sizeOfIntInBits)
        {
            return DataTypeProvider::provideDataType(PhysicalType::Type::INT32);
        }

        if (leftBits == sizeOfIntInBits and rightBits < sizeOfIntInBits)
        {
            return (
                left.isSignedInteger() ? DataTypeProvider::provideDataType(PhysicalType::Type::INT32)
                                       : DataTypeProvider::provideDataType(PhysicalType::Type::UINT32));
        }

        if (leftBits < sizeOfIntInBits and rightBits == sizeOfIntInBits)
        {
            return (
                right.isSignedInteger() ? DataTypeProvider::provideDataType(PhysicalType::Type::INT32)
                                        : DataTypeProvider::provideDataType(PhysicalType::Type::UINT32));
        }

        if (leftBits == sizeOfIntInBits and rightBits == sizeOfIntInBits)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger()) ? DataTypeProvider::provideDataType(PhysicalType::Type::INT32)
                                                                     : DataTypeProvider::provideDataType(PhysicalType::Type::UINT32));
        }

        if (leftBits == sizeOfLongInBits and rightBits < sizeOfLongInBits)
        {
            return (
                left.isSignedInteger() ? DataTypeProvider::provideDataType(PhysicalType::Type::INT64)
                                       : DataTypeProvider::provideDataType(PhysicalType::Type::UINT64));
        }

        if (leftBits < sizeOfLongInBits and rightBits == sizeOfLongInBits)
        {
            return (
                right.isSignedInteger() ? DataTypeProvider::provideDataType(PhysicalType::Type::INT64)
                                        : DataTypeProvider::provideDataType(PhysicalType::Type::UINT64));
        }

        if (leftBits == sizeOfLongInBits and rightBits == sizeOfLongInBits)
        {
            return (
                (left.isSignedInteger() and right.isSignedInteger()) ? DataTypeProvider::provideDataType(PhysicalType::Type::INT64)
                                                                     : DataTypeProvider::provideDataType(PhysicalType::Type::UINT64));
        }
    }

    return {};
}

DataType DataType::join(const DataType& otherDataType)
{
    if (this->physicalType.type == PhysicalType::Type::UNDEFINED or this->physicalType.type == PhysicalType::Type::VARSIZED)
    {
        return DataTypeProvider::provideDataType(PhysicalType::Type::UNDEFINED);
    }

    if (this->isNumeric())
    {
        if (otherDataType.physicalType.type == PhysicalType::Type::UNDEFINED)
        {
            return DataType{PhysicalType{physicalType}};
        }

        if (not otherDataType.isNumeric())
        {
            // Todo: does this actually print correctly?
            // throw DifferentFieldTypeExpected("Cannot join {} and {}", "*this", otherDataType);
        }

        if (const auto newDataType = inferNumericDataType(*this, otherDataType); newDataType.has_value())
        {
            return newDataType.value();
        }
        // throw DifferentFieldTypeExpected("Cannot join {} and {}", "*this", otherDataType);
    }
    if (this->physicalType.type == PhysicalType::Type::CHAR)
    {
        if (otherDataType.physicalType.type == PhysicalType::Type::CHAR)
        {
            return DataTypeProvider::provideDataType(PhysicalType::Type::CHAR);
        }
        return DataTypeProvider::provideDataType(PhysicalType::Type::UNDEFINED);
    }
    if (this->physicalType.type == PhysicalType::Type::BOOLEAN)
    {
        if (otherDataType.physicalType.type == PhysicalType::Type::BOOLEAN)
        {
            return DataTypeProvider::provideDataType(PhysicalType::Type::BOOLEAN);
        }
        return DataTypeProvider::provideDataType(PhysicalType::Type::UNDEFINED);
    }
    throw DifferentFieldTypeExpected("Cannot and ");
    // throw DifferentFieldTypeExpected("Cannot join {} and {}", "*this", otherDataType);
}
std::ostream& operator<<(std::ostream& os, const DataType& dataType)
{
    return os << fmt::format("DataType(PhysicalType({}))", dataType.physicalType);
}
}
