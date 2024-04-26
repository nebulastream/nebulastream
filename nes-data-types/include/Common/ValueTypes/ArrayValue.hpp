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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_VALUETYPES_ARRAYVALUE_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_VALUETYPES_ARRAYVALUE_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <type_traits>
#include <vector>

namespace NES {

class [[nodiscard]] ArrayValue final : public ValueType {
  public:
    inline ArrayValue(DataTypePtr type, std::vector<std::string>&& values) noexcept
        : ValueType(type), values(std::move(values)) {}

    ~ArrayValue() override = default;

    /// @brief Returns a string representation of this value
    [[nodiscard]] std::string toString() const noexcept override;

    /// @brief Checks if two values are equal
    [[nodiscard]] bool isEquals(ValueTypePtr other) const noexcept override;

    std::vector<std::string> const values;
};

}// namespace NES

#endif // NES_DATA_TYPES_INCLUDE_COMMON_VALUETYPES_ARRAYVALUE_HPP_
