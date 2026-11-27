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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <ranges>
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

DataType::DataType() : type(Type::UNDEFINED), nullable(true), typeName(std::nullopt)
{
}

DataType::DataType(const Type type, const NULLABLE nullable, std::optional<std::string> typeName)
    : type(type), nullable(nullable == NULLABLE::IS_NULLABLE), typeName(std::move(typeName))
{
}

DataType::DataType(const Type type, const NULLABLE nullable, std::string typeName, std::vector<std::pair<std::string, DataType>> fields)
    : type(type), nullable(nullable == NULLABLE::IS_NULLABLE), typeName(std::move(typeName)), fields(std::move(fields))
{
    if (type != Type::STRUCT)
    {
        throw DifferentFieldTypeExpected("The fields DataType constructor is for STRUCT only, but got: {}", magic_enum::enum_name(type));
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
        case Type::INT64:
        case Type::UINT64:
        case Type::FLOAT64:
            return 8;
        case Type::STRUCT: {
            /// Since a struct is stored inline field-by-field, the size is the sum over the field sizes.
            uint32_t structSize = 0;
            for (const auto& fieldType : fields | std::views::values)
            {
                structSize += fieldType.getSizeInBytesWithoutNull();
            }
            return structSize;
        }
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

bool DataType::isType(const Type type) const
{
    return this->type == type;
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

bool DataType::isFlat() const
{
    switch (this->type)
    {
        case Type::INT8:
        case Type::INT16:
        case Type::INT32:
        case Type::INT64:
        case Type::UINT8:
        case Type::UINT16:
        case Type::UINT32:
        case Type::UINT64:
        case Type::FLOAT32:
        case Type::FLOAT64:
        case Type::BOOLEAN:
        case Type::CHAR:
        case Type::UNDEFINED:
            return true;
        case Type::VARSIZED:
            return false;
        case Type::STRUCT: {
            /// A struct is flat / may be flattened, if it does not contain any non-flattable elements
            for (const auto& fieldType : fields | std::views::values)
            {
                if (not fieldType.isFlat())
                {
                    return false;
                }
            }
            return true;
        }
    }
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
    /// Nominal Typing: DataTypes that do not share a name must not be joined.
    if (typeName != otherDataType.typeName)
    {
        NES_WARNING("Cannot join {} and {}, as their names are not equal.", *this, otherDataType);
        return std::nullopt;
    }
    if (typeName.has_value() && otherDataType.typeName.has_value())
    {
        /// Both types hold an equal name.
        /// Their members (besides isNull) must all be equal.
        if (this->fields != otherDataType.fields || this->type != otherDataType.type)
        {
            NES_WARNING("Cannot join {} and {}, as they have the same name but do not have equal properties", *this, otherDataType);
            return std::nullopt;
        }
        /// Provide joined data type with the type name as key.
        return {DataTypeProvider::provideDataType(*this->typeName, isNullableResult)};
    }
    /// Join conditions for unnamed types.

    if (this->type == Type::UNDEFINED)
    {
        return {DataTypeProvider::provideDataType(Type::UNDEFINED, isNullableResult)};
    }
    if (this->type == Type::VARSIZED)
    {
        return (otherDataType.isType(Type::VARSIZED)) ? std::optional{DataTypeProvider::provideDataType(Type::VARSIZED, isNullableResult)}
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
            return {DataTypeProvider::provideDataType(Type::CHAR, isNullableResult)};
        }
        NES_WARNING("Cannot join {} and {}", *this, otherDataType);
        return std::nullopt;
    }
    if (this->type == Type::BOOLEAN)
    {
        if (otherDataType.type == Type::BOOLEAN)
        {
            return {DataTypeProvider::provideDataType(Type::BOOLEAN, isNullableResult)};
        }
        NES_WARNING("Cannot join {} and {}", *this, otherDataType);
        return std::nullopt;
    }
    NES_WARNING("Cannot join {} and {}", *this, otherDataType);
    return std::nullopt;
}

Reflected Reflector<DataType>::operator()(const DataType& field, const ReflectionContext& context) const
{
    const detail::ReflectedDataType reflected{
        .type = field.type, .nullable = field.nullable, .typeName = field.typeName, .fields = field.fields};
    return context.reflect(reflected);
}

DataType Unreflector<DataType>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    const auto reflected = context.unreflect<detail::ReflectedDataType>(rfl);
    const auto nullableEnum = reflected.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    if (reflected.type == DataType::Type::STRUCT)
    {
        /// STRUCT types are always named, we can access the typeName without hasValue check.
        return DataType{reflected.type, nullableEnum, *reflected.typeName, reflected.fields};
    }
    return DataType{reflected.type, nullableEnum, reflected.typeName};
}

std::ostream& operator<<(std::ostream& os, const DataType& dataType)
{
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
        return os << fmt::format("DataType(name: {} type: STRUCT{{{}}} nullable: {})", *dataType.typeName, fieldList, dataType.nullable);
    }
    if (dataType.typeName.has_value())
    {
        return os << fmt::format(
                   "DataType(name: {} type: {} nullable: {})", *dataType.typeName, magic_enum::enum_name(dataType.type), dataType.nullable);
    }
    return os << fmt::format("DataType(type: {} nullable: {})", magic_enum::enum_name(dataType.type), dataType.nullable);
}

}
