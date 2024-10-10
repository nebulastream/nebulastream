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

#include <cstdint>
#include <type_traits>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::PhysicalTypes
{

/**
 * @brief Function to check if the physical type is a char
 * @param physicalType
 * @return true if the physical type is a char
 */
bool isChar(PhysicalTypePtr physicalType);


bool isVariableSizedData(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a bool
 * @param physicalType
 * @return true if the physical type is a bool
 */
bool isBool(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a unsigned int 8
 * @param physicalType
 * @return true if the physical type is a unsigned int 8
 */
bool isUInt8(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a unsigned int 16
 * @param physicalType
 * @return true if the physical type is a unsigned int 16
 */
bool isUInt16(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a unsigned int 32
 * @param physicalType
 * @return true if the physical type is a unsigned int 32
 */
bool isUInt32(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a unsigned int 64
 * @param physicalType
 * @return true if the physical type is a unsigned int 64
 */
bool isUInt64(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a int 8
 * @param physicalType
 * @return true if the physical type is a int 8
 */
bool isInt8(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a int 16
 * @param physicalType
 * @return true if the physical type is a int 16
 */
bool isInt16(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a int 32
 * @param physicalType
 * @return true if the physical type is a int 32
 */
bool isInt32(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a int 64
 * @param physicalType
 * @return true if the physical type is a int 64
 */
bool isInt64(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a float
 * @param physicalType
 * @return true if the physical type is a float
 */
bool isFloat(PhysicalTypePtr physicalType);

/**
 * @brief Function to check if the physical type is a double
 * @param physicalType
 * @return true if the physical type is a double
 */
bool isDouble(PhysicalTypePtr physicalType);

/**
 * @brief Function to check that a compile-time type is the same as a particular physical type.
 * Depending on the compile-time type parameter, this method selects the suitable type check on the physical type at runtime.
 * @tparam Type parameter
 * @param physicalType the physical type at runtime
 * @return returns true if the type is correct.
 */
template <class Type>
bool isSamePhysicalType(PhysicalTypePtr physicalType)
{
    if (isVariableSizedData(physicalType) && std::is_same_v<std::remove_cvref_t<Type>, std::uint32_t>)
    {
        return true;
    }
    if constexpr (std::is_same_v<std::remove_cvref_t<Type>, char>)
    {
        return isChar(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint8_t>)
    {
        return isUInt8(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, bool>)
    {
        return isBool(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint16_t>)
    {
        return isUInt16(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint32_t>)
    {
        return isUInt32(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint64_t>)
    {
        return isUInt64(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int8_t>)
    {
        return isInt8(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int16_t>)
    {
        return isInt16(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int32_t>)
    {
        return isInt32(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int64_t>)
    {
        return isInt64(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, float>)
    {
        return isFloat(std::move(physicalType));
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, double>)
    {
        return isDouble(std::move(physicalType));
    }
    return false;
}
}
