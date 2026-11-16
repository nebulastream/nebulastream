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

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>

#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>
#include "Types/NESType.hpp"

namespace NES
{

/// Open trait attached to all scalar `NESType`s (both built-in `DataType`s and custom scalar type constructors).
struct ScalarTypeTrait
{
    std::function<std::optional<struct DataType>(const ExplicitAny&)> tryGetDataType;
};

/// Canonical scalar literal payload carried by scalar constant `NESValue`s.
using ScalarLiteralValue = std::variant<std::string, int64_t, uint64_t, double, bool, std::monostate>;

struct DataType final
{
    enum class Type : uint8_t
    {
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT32,
        FLOAT64,
        BOOLEAN,
        CHAR,
        UNDEFINED,
        VARSIZED,
    };

    enum class NULLABLE : uint8_t
    {
        IS_NULLABLE,
        NOT_NULLABLE
    };

    DataType(Type type, NULLABLE nullable);
    DataType();

    template <class T>
    [[nodiscard]] bool isSameDataType() const
    {
        if constexpr (std::is_same_v<std::remove_cvref_t<T>, bool>)
        {
            return this->type == Type::BOOLEAN;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, char>)
        {
            return this->type == Type::CHAR;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint8_t>)
        {
            return this->type == Type::UINT8;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint16_t>)
        {
            return this->type == Type::UINT16;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint32_t>)
        {
            return this->type == Type::UINT32;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint64_t>)
        {
            return this->type == Type::UINT64;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int8_t>)
        {
            return this->type == Type::INT8;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int16_t>)
        {
            return this->type == Type::INT16;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int32_t>)
        {
            return this->type == Type::INT32;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int64_t>)
        {
            return this->type == Type::INT64;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, float>)
        {
            return this->type == Type::FLOAT32;
        }
        else if constexpr (std::is_same_v<std::remove_cvref_t<T>, double>)
        {
            return this->type == Type::FLOAT64;
        }
        return false;
    }

    bool operator==(const DataType& other) const = default;
    bool operator!=(const DataType& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const DataType& dataType);

    /// Provides the size needed for storing this data type containing any additional space, e.g., for null-handling
    [[nodiscard]] uint32_t getSizeInBytesWithNull() const;

    /// Provides the raw underlying size. This means the raw data type without any additional space, e.g., for null-handling
    [[nodiscard]] uint32_t getSizeInBytesWithoutNull() const;

    /// Determines common data type for this and other data type. Returns @Type::UNDEFINED if it cannot find a common type.
    /// example usage a binary arithmetical function: 'const auto commonStamp = left->getStamp().join(right->getStamp());'
    [[nodiscard]] std::optional<DataType> join(const DataType& otherDataType) const;
    [[nodiscard]] DataType::NULLABLE joinNullable(const DataType& otherDataType) const;

    [[nodiscard]] bool isType(Type type) const;
    [[nodiscard]] bool isInteger() const;
    [[nodiscard]] bool isSignedInteger() const;
    [[nodiscard]] bool isFloat() const;
    [[nodiscard]] bool isNumeric() const;

    Type type;
    bool nullable;

    [[nodiscard]] NESType toNESType() const;
    static NESValue makeBooleanValue(bool value);
    static NESValue makeIntegerValue(int64_t value);
    static NESFunctor makeRegistryFunctor(std::function<DataType(struct DataTypeRegistryArguments)> factory);
    static NESFunctor functorOf(DataType::Type type);
};

template <>
struct Reflector<DataType>
{
    Reflected operator()(const DataType& field, const ReflectionContext& context) const;
};

template <>
struct Unreflector<DataType>
{
    DataType operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::DataType>
{
    size_t operator()(const NES::DataType& dataType) const noexcept
    {
        return (static_cast<uint16_t>(dataType.type) << 8) | static_cast<uint8_t>(dataType.nullable);
    }
};

FMT_OSTREAM(NES::DataType);
