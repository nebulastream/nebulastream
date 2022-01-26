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

#ifndef NES_INCLUDE_COMMON_VALUETYPES_ARRAYVALUE_HPP_
#define NES_INCLUDE_COMMON_VALUETYPES_ARRAYVALUE_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <type_traits>
#include <vector>

namespace NES {

class [[nodiscard]] ArrayValue final : public ValueType {
  public:
    inline ArrayValue(DataTypePtr && type, std::vector<std::string> && values) noexcept
        : ValueType(std::move(type)), values(std::move(values)) {}

    virtual ~ArrayValue() = default;

    /// @brief Returns a string representation of this value
    [[nodiscard]] std::string toString() const noexcept final;

    /// @brief Checks if two values are equal
    [[nodiscard]] bool isEquals(ValueTypePtr other) const noexcept final;

    std::vector<std::string> const values;
};

}// namespace NES

#endif  // NES_INCLUDE_COMMON_VALUETYPES_ARRAYVALUE_HPP_
