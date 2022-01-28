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

#ifndef NES_INCLUDE_COMMON_PHYSICALTYPES_PHYSICALTYPEUTIL_HPP_
#define NES_INCLUDE_COMMON_PHYSICALTYPES_PHYSICALTYPEUTIL_HPP_

#include <Common/ExecutableType/NESType.hpp>
#include <cstdint>

namespace NES::PhysicalTypes {

/**
 * @brief Function to check if the physical type is a char
 * @param physicalType
 * @return true if the physical type is a char
 */
bool isChar(PhysicalTypePtr physicalType);

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
 * @brief Function to check if the physical type is a array
 * @param physicalType
 * @return true if the physical type is a array
 */
bool isArray(PhysicalTypePtr physicalType);

/**
 * @brief Gets the component physical type of an array
 * @param physicalType
 * @return true if the physical type of an array
 */
PhysicalTypePtr getArrayComponent(PhysicalTypePtr physicalType);

/**
 * @brief Function to check that a compile-time type is the same as a particular physical type.
 * Depending on the compile-time type parameter, this method selects the suitable type check on the physical type at runtime.
 * @tparam Type parameter
 * @param physicalType the physical type at runtime
 * @return returns true if the type is correct.
 */
template<class Type>
bool isSamePhysicalType(PhysicalTypePtr physicalType) {
    if constexpr (IsChar<Type>) {
        return isChar(std::move(physicalType));
    } else if constexpr (IsUInt8<Type>) {
        return isUInt8(std::move(physicalType));
    } else if constexpr (IsBool<Type>) {
        return isBool(std::move(physicalType));
    } else if constexpr (IsUInt16<Type>) {
        return isUInt16(std::move(physicalType));
    } else if constexpr (IsUInt32<Type>) {
        return isUInt32(std::move(physicalType));
    } else if constexpr (IsUInt64<Type>) {
        return isUInt64(std::move(physicalType));
    } else if constexpr (IsInt8<Type>) {
        return isInt8(std::move(physicalType));
    } else if constexpr (IsInt16<Type>) {
        return isInt16(std::move(physicalType));
    } else if constexpr (IsInt32<Type>) {
        return isInt32(std::move(physicalType));
    } else if constexpr (IsInt64<Type>) {
        return isInt64(std::move(physicalType));
    } else if constexpr (IsFloat<Type>) {
        return isFloat(std::move(physicalType));
    } else if constexpr (IsDouble<Type>) {
        return isDouble(std::move(physicalType));
    } else if constexpr (std::is_pointer_v<Type>) {
        return isArray(physicalType) && isSamePhysicalType<std::remove_pointer_t<Type>>(getArrayComponent(physicalType));
    } else if constexpr (std::is_base_of_v<NESType, Type>) {
        return physicalType->isArrayType();
    }
    return false;
}
}// namespace NES::PhysicalTypes

#endif  // NES_INCLUDE_COMMON_PHYSICALTYPES_PHYSICALTYPEUTIL_HPP_
