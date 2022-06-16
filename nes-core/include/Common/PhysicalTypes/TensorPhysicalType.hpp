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

#ifndef NES_INCLUDE_COMMON_PHYSICALTYPES_TENSORPHYSICALTYPE_HPP
#define NES_INCLUDE_COMMON_PHYSICALTYPES_TENSORPHYSICALTYPE_HPP

#include <Common/DataTypes/TensorType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <iostream>

namespace NES {

class TensorPhysicalType final : public PhysicalType {

  public:
    inline TensorPhysicalType(DataTypePtr dataType,
                              std::vector<size_t> shape,
                              PhysicalTypePtr component,
                              TensorMemoryFormat tensorMemoryFormat) noexcept
        : PhysicalType(std::move(dataType)), shape(std::move(shape)), physicalComponentType(std::move(component)),
          tensorMemoryFormat(tensorMemoryFormat) {
        for (auto dimension : this->shape) {
            totalSize *= dimension;
        }
    }

    virtual ~TensorPhysicalType() = default;

    /**
     * @brief Factory function to create a new ArrayType Physical Type.
     * @param type the logical data type.
     * @param length the length of the array.
     * @param component the physical component type of this array.
     * @return PhysicalTypePtr
     */
    static inline PhysicalTypePtr create(const DataTypePtr& type,
                                         std::vector<size_t> shape,
                                         PhysicalTypePtr const& component,
                                         TensorMemoryFormat tensorMemoryFormat) noexcept {
        return std::make_shared<TensorPhysicalType>(type, shape, component, tensorMemoryFormat);
    }

    /**
    * @brief Indicates if this is a tensor data type.
    * @return true if type is tensor
    */
    [[nodiscard]] bool isTensorType() const noexcept final { return true; }

    /**
     * @brief Returns the number of bytes occupied by this data type.
     * @return uint64_t
     */
    [[nodiscard]] uint64_t size() const final;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    std::string convertRawToString(void const* rawData) const noexcept final;

    /**
     * @brief Converts the binary representation of this value to a string without filling
     * up the difference between the length of the string and the end of the schema definition
     * with unrelated characters
     * @param rawData a pointer to the raw value
     * @return string
    */
    std::string convertRawToStringWithoutFill(void const* rawData) const noexcept final;

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    [[nodiscard]] std::string toString() const noexcept final;

    size_t totalSize = 1;
    const std::vector<size_t> shape;
    PhysicalTypePtr const physicalComponentType;
    TensorMemoryFormat tensorMemoryFormat;
};

}// namespace NES

#endif//NES_INCLUDE_COMMON_PHYSICALTYPES_TENSORPHYSICALTYPE_HPP
