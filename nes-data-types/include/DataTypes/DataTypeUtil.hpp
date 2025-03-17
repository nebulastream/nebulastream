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

#include <DataTypes/DataType.hpp>
#include <boost/asio/detail/socket_option.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::DataTypeUtil
{

template <DataType::Type EnumType>
struct TypeMapping;

template <>
struct TypeMapping<DataType::Type::INT8>
{
    using type = int8_t;
};
template <>
struct TypeMapping<DataType::Type::INT16>
{
    using type = int16_t;
};
template <>
struct TypeMapping<DataType::Type::INT32>
{
    using type = int32_t;
};
template <>
struct TypeMapping<DataType::Type::INT64>
{
    using type = int64_t;
};
template <>
struct TypeMapping<DataType::Type::UINT8>
{
    using type = uint8_t;
};
template <>
struct TypeMapping<DataType::Type::UINT16>
{
    using type = uint16_t;
};
template <>
struct TypeMapping<DataType::Type::UINT32>
{
    using type = uint32_t;
};
template <>
struct TypeMapping<DataType::Type::UINT64>
{
    using type = uint64_t;
};
template <>
struct TypeMapping<DataType::Type::FLOAT32>
{
    using type = float;
};
template <>
struct TypeMapping<DataType::Type::FLOAT64>
{
    using type = double;
};
template <>
struct TypeMapping<DataType::Type::BOOLEAN>
{
    using type = bool;
};
template <>
struct TypeMapping<DataType::Type::CHAR>
{
    using type = char;
};

template <DataType::Type EnumType>
using TypeMappingT = typename TypeMapping<EnumType>::type;
template <DataType::Type EnumType>
constexpr auto getTypeFromEnum()
{
    return TypeMapping<EnumType>();
}

/// Expects a numerical type.
/// @throws if instantiated with a non-numerical data type
template <typename Func>
auto dispatchByNumericalType(const DataType::Type type, Func&& func)
{
    switch (type)
    {
        case DataType::Type::INT8:
            return func.template operator()<int8_t>();
        case DataType::Type::INT16:
            return func.template operator()<int16_t>();
        case DataType::Type::INT32:
            return func.template operator()<int32_t>();
        case DataType::Type::INT64:
            return func.template operator()<int64_t>();
        case DataType::Type::UINT8:
            return func.template operator()<uint8_t>();
        case DataType::Type::UINT16:
            return func.template operator()<uint16_t>();
        case DataType::Type::UINT32:
            return func.template operator()<uint32_t>();
        case DataType::Type::UINT64:
            return func.template operator()<uint64_t>();
        case DataType::Type::FLOAT32:
            return func.template operator()<float>();
        case DataType::Type::FLOAT64:
            return func.template operator()<double>();
        default:
            throw UnknownPhysicalType("Cannot dispatch for type: {}", magic_enum::enum_name(type));
    }
}

/// Expects a numerical type.
/// @throws if instantiated with a non-numerical data type or bool
template <typename Func>
auto dispatchByNumericalOrBoolType(const DataType::Type type, Func&& func)
{
    if (type == DataType::Type::BOOLEAN)
    {
        return func.template operator()<bool>();
    }
    return dispatchByNumericalType<Func>(type, std::forward<Func>(func));
}

/// Expects a numerical type.
/// @throws if instantiated with a non-numerical data type or bool or char (does not accept varchar)
template <typename Func>
auto dispatchByNumericalOrBoolOrCharType(const DataType::Type type, Func&& func)
{
    if (type == DataType::Type::CHAR)
    {
        return func.template operator()<char>();
    }
    return dispatchByNumericalOrBoolType<Func>(type, std::forward<Func>(func));
}


template <typename T>
bool isSameDataType(const DataType::Type type)
{
    if (type == DataType::Type::VARSIZED and std::is_same_v<std::remove_cvref_t<T>, std::uint32_t>)
    {
        return true;
    }
    if constexpr (std::is_same_v<std::remove_cvref_t<T>, char>)
    {
        return type == DataType::Type::CHAR;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint8_t>)
    {
        return type == DataType::Type::UINT8;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, bool>)
    {
        return type == DataType::Type::BOOLEAN;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint16_t>)
    {
        return type == DataType::Type::UINT16;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint32_t>)
    {
        return type == DataType::Type::UINT32;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::uint64_t>)
    {
        return type == DataType::Type::UINT64;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int8_t>)
    {
        return type == DataType::Type::INT8;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int16_t>)
    {
        return type == DataType::Type::INT16;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int32_t>)
    {
        return type == DataType::Type::INT32;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, std::int64_t>)
    {
        return type == DataType::Type::INT64;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, float>)
    {
        return type == DataType::Type::FLOAT32;
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<T>, double>)
    {
        return type == DataType::Type::FLOAT64;
    }
    return false;
}
}
