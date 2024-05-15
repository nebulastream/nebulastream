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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_PHYSICALTYPES_TEXTPHYSICALTYPE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_PHYSICALTYPES_TEXTPHYSICALTYPE_HPP_

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <utility>

namespace NES {

/**
 * @brief The text physical type, which represent TextType and FixedChar types in NES.
 */
class TextPhysicalType final : public PhysicalType {

  public:
    /**
     * @brief Factory function to create a new TextType Physical Type.
     * @param type the logical data type.
     * @param length the length of the text.
     * @param component the physical component type of this text.
     */
     // Todo: what do we need the component for? -> don't all relevant function take a pointer to the component anyway?
    inline TextPhysicalType(DataTypePtr type) noexcept : PhysicalType(std::move(type)) {}

    ~TextPhysicalType() override = default;

    /**
     * @brief Factory function to create a new TextType Physical Type.
     * @param type the logical data type.
     * @param length the length of the text.
     * @param component the physical component type of this text.
     * @return PhysicalTypePtr
     */
    static inline PhysicalTypePtr create(const DataTypePtr& type) noexcept {
        return std::make_shared<TextPhysicalType>(type);
    }

    /**
     * TODO: use dynamic_cast instead of 'isXType()'
//    * @brief Indicates if this is a text data type.
//    * @return true if type is text
//    */
    [[nodiscard]] bool isTextType() const noexcept override { return true; }

    /**
     * @brief Returns the length of the string, which is the number of bytes occupied by the string.
     * @return uint64_t
     */
    [[nodiscard]] uint64_t size() const override;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    std::string convertRawToString(void const* rawData) const noexcept override;

    /**
     * @brief Converts the binary representation of this value to a string without filling
     * up the difference between the length of the string and the end of the schema definition
     * with unrelated characters
     * @param rawData a pointer to the raw value
     * @return string
    */
    std::string convertRawToStringWithoutFill(void const* rawData) const noexcept override;

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    [[nodiscard]] std::string toString() const noexcept override;
};

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_PHYSICALTYPES_TEXTPHYSICALTYPE_HPP_
