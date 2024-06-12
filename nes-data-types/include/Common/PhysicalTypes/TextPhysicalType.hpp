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
    inline TextPhysicalType(DataTypePtr type) noexcept : PhysicalType(std::move(type)) {}

    ~TextPhysicalType() override = default;

    static inline PhysicalTypePtr create(const DataTypePtr& type) noexcept { return std::make_shared<TextPhysicalType>(type); }

    [[nodiscard]] bool isTextType() const noexcept override { return true; }

    [[nodiscard]] uint64_t size() const override;

    std::string convertRawToString(void const* rawData) const noexcept override;

    std::string convertRawToStringWithoutFill(void const* rawData) const noexcept override;

    [[nodiscard]] std::string toString() const noexcept override;
};

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_PHYSICALTYPES_TEXTPHYSICALTYPE_HPP_
