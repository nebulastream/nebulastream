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
#include <Util/Box.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>

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
        VECTOR
    };

    enum class NULLABLE : uint8_t
    {
        IS_NULLABLE,
        NOT_NULLABLE
    };

    DataType(Type type, NULLABLE nullable);
    /// Todo: remove in a proper frontend datatype refactoring
    /// Constructor for vectors -> no fixed size but an element type
    DataType(Type type, NULLABLE nullable, DataType elementType);
    /// /// FIXEDSIZED-only constructor: also carries element type and count. The element type can be any DataType, enabling nesting.
    DataType(Type type, NULLABLE nullable, DataType elementType, uint32_t count);
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
    /// Returns whether this data type can be stored entirely inlined, without pointers to a child-buffer for varsized contents.
    /// This is true for our base types as well as STRUCT and FIXESIZED types with only flat members.
    [[nodiscard]] bool isFlat() const;
    /// For a datatype, get the maximum amount of nested variable-sized types.
    /// For example: INT32 -> 0, VARSIZED -> 1, INT32 ARRAY[3] -> 0, INT32 ARRAY[][] -> 2, INT32 ARRAY[][2][] -> 2, STRUCT{INT32, INT32 ARRAY[][], INT32 ARRAY} -> 2, STRUCT{STRUCT{BOOLEAN, VARSIZED}, INT32} -> 1.
    /// Currently, any type with a varsized nesting depth > 1 is not supported.
    [[nodiscard]] uint32_t getVarsizedNestingDepth() const;

    /// A registered struct plugin or an array type is "valid", if its maximum varsized nesting depth is < 2;
    [[nodiscard]] bool isValid() const { return getVarsizedNestingDepth() < 2; }

    Type type;
    bool nullable;
    /// Only set when `type == FIXEDSIZED`; empty otherwise. Boxed, as a DataType cannot directly contain another DataType.
    /// Box compares deeply, so the defaulted comparison operators keep value semantics.
    Box<DataType> elementType;
    uint32_t count = 0;
    /// Only meaningful when `type == STRUCT`. Identity is nominal: two STRUCTs
    /// are equal if name and fields match.
    std::string structName;
    std::vector<std::pair<std::string, DataType>> fields;
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

namespace NES::detail
{
/// Flat, reflectable mirror of DataType. All variant-specific members are always
/// present; which ones are meaningful is decided by `type` on the way back
/// (mirrors the DataType constructors). Reflecting an aggregate serializes to a
/// named object, so the wire form is self-documenting and order-independent.
struct ReflectedDataType
{
    DataType::Type type;
    bool nullable;
    std::optional<DataType> elementType;
    uint32_t count;
    std::string structName;
    std::vector<std::pair<std::string, DataType>> fields;
};
}

template <>
struct std::hash<NES::DataType>
{
    size_t operator()(const NES::DataType& dataType) const noexcept
    {
        size_t h = (static_cast<uint16_t>(dataType.type) << 8) | static_cast<uint8_t>(dataType.nullable);
        if (dataType.elementType.hasValue())
        {
            h ^= std::hash<NES::DataType>{}(*dataType.elementType) + 0x9e3779b9 + (h << 6) + (h >> 2);
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
