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
#include <utility>
#include <vector>
#include <Util/Logger/Formatter.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

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
        FIXEDSIZED,
        STRUCT,
        VARARRAY
    };

    enum class NULLABLE : uint8_t
    {
        IS_NULLABLE,
        NOT_NULLABLE
    };

    DataType(Type type, NULLABLE nullable);
    /// Constructor for vararrays -> no fixed size but an element type
    DataType(Type type, NULLABLE nullable, const DataType& elementType);
    /// FIXEDSIZED-only constructor: also carries element type and count.
    /// Todo: remove in a proper frontend datatype refactoring
    DataType(Type type, NULLABLE nullable, const DataType& elementType, uint32_t count);
    /// STRUCT-only constructor: nominal name + ordered named field layout.
    /// Hacky PoC for extensible composite types — plugins register a creator that
    /// builds one of these for their named struct (e.g. "Image").
    DataType(Type type, NULLABLE nullable, std::string structName, std::vector<std::pair<std::string, DataType>> fields);
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
    /// Does the type have a fixed size? Used to determine whether the type requires child buffers to be stored in.
    [[nodiscard]] bool isFixedSized() const;

    Type type;
    bool nullable;
    /// Only meaningful when `type == FIXEDSIZED` or `type == VARARRAY`; defaults make non-array DataTypes
    /// compare and hash identically to before this struct grew these fields.
    /// For poc, store as size 1 vector, as a workaround for DataType to store its element datatype
    std::vector<DataType> elementType;
    uint32_t count = 0;
    /// Only meaningful when `type == STRUCT`. Identity is nominal: two STRUCTs
    /// are equal iff name and fields match.
    std::string structName;
    std::vector<std::pair<std::string, DataType>> fields;
};

template <>
struct Reflector<DataType>
{
    Reflected operator()(const DataType& field) const;
};

template <>
struct Unreflector<DataType>
{
    DataType operator()(const Reflected& rfl) const;
};

}

template <>
struct std::hash<NES::DataType>
{
    size_t operator()(const NES::DataType& dataType) const noexcept
    {
        size_t h = (static_cast<uint16_t>(dataType.type) << 8) | static_cast<uint8_t>(dataType.nullable);
        for (const auto& element : dataType.elementType)
        {
            h ^= std::hash<NES::DataType>{}(element) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        h ^= static_cast<size_t>(dataType.count) << 24;
        h ^= std::hash<std::string>{}(dataType.structName) << 1;
        for (const auto& [name, field] : dataType.fields)
        {
            h ^= std::hash<std::string>{}(name) + 0x9e3779b9 + (h << 6) + (h >> 2);
            h ^= std::hash<NES::DataType>{}(field) + 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        return h;
    }
};

FMT_OSTREAM(NES::DataType);
