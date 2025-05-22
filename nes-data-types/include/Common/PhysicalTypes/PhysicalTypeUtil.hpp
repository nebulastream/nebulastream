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
#include <memory>
#include <type_traits>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::PhysicalTypes
{

bool isSpecificBasicType(const PhysicalType& physicalType, BasicPhysicalType::NativeType specificBasicType);

bool isChar(const PhysicalType& physicalType);

bool isVariableSizedData(const std::shared_ptr<PhysicalType>& physicalType);

bool isBool(const PhysicalType& physicalType);

bool isUInt8(const PhysicalType& physicalType);

bool isUInt16(const PhysicalType& physicalType);

bool isUInt32(const PhysicalType& physicalType);

bool isUInt64(const PhysicalType& physicalType);

bool isInt8(const PhysicalType& physicalType);

bool isInt16(const PhysicalType& physicalType);

bool isInt32(const PhysicalType& physicalType);

bool isInt64(const PhysicalType& physicalType);

bool isFloat(const PhysicalType& physicalType);

bool isDouble(const PhysicalType& physicalType);

/// Function to check that a compile-time type is the same as a particular physical type.
/// Depending on the compile-time type parameter, this method selects the suitable type check on the physical type at runtime.
/// @tparam Type parameter
/// @param physicalType the physical type at runtime
/// @return returns true if the type is correct.
template <class Type>
bool isSamePhysicalType(const std::shared_ptr<PhysicalType>& physicalType)
{
    if (isVariableSizedData(physicalType) && std::is_same_v<std::remove_cvref_t<Type>, std::uint32_t>)
    {
        return true;
    }
    if constexpr (std::is_same_v<std::remove_cvref_t<Type>, char>)
    {
        return isChar(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint8_t>)
    {
        return isUInt8(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, bool>)
    {
        return isBool(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint16_t>)
    {
        return isUInt16(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint32_t>)
    {
        return isUInt32(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::uint64_t>)
    {
        return isUInt64(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int8_t>)
    {
        return isInt8(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int16_t>)
    {
        return isInt16(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int32_t>)
    {
        return isInt32(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, std::int64_t>)
    {
        return isInt64(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, float>)
    {
        return isFloat(*physicalType);
    }
    else if constexpr (std::is_same_v<std::remove_cvref_t<Type>, double>)
    {
        return isDouble(*physicalType);
    }
    return false;
}
}
