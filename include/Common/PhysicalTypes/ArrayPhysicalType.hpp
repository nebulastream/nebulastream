/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES {

/**
 * @brief The array physical type, which represent ArrayType and FixedChar types in NES.
 */
class ArrayPhysicalType final : public PhysicalType {

  public:
    /**
     * @brief Factory function to create a new ArrayType Physical Type.
     * @param type the logical data type.
     * @param length the length of the array.
     * @param component the physical component type of this array.
     */
    inline ArrayPhysicalType(DataTypePtr type, uint64_t length, PhysicalTypePtr component) noexcept
        : PhysicalType(type), length(length), physicalComponentType(component) {}

    /**
     * @brief Factory function to create a new ArrayType Physical Type.
     * @param type the logical data type.
     * @param length the length of the array.
     * @param component the physical component type of this array.
     * @return PhysicalTypePtr
     */
    static inline PhysicalTypePtr create(DataTypePtr type, uint64_t length, PhysicalTypePtr const& component) noexcept {
        return std::make_shared<ArrayPhysicalType>(type, length, component);
    }

    /**
    * @brief Indicates if this is a array data type.
    * @return true if type is array
    */
    bool isArrayType() const noexcept final { return true; }

    /// @brief Return if this array directly contains chars.
    bool isCharArrayType() const noexcept final;

    /**
     * @brief Returns the number of bytes occupied by this data type.
     * @return u_int64_t
     */
    uint64_t size() const final;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    std::string convertRawToString(void const* rawData) const noexcept final;

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    std::string toString() const noexcept final;

    /// Fixed number of elements of type `physicalComponentType` contained in the array.
    uint64_t const length;

    /// Type
    PhysicalTypePtr const physicalComponentType;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_ARRAYPHYSICALTYPE_HPP_
