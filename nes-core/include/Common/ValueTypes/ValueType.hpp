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

#ifndef NES_INCLUDE_COMMON_VALUETYPES_VALUETYPE_HPP_
#define NES_INCLUDE_COMMON_VALUETYPES_VALUETYPE_HPP_

#include <bitset>
#include <memory>
#include <string>

namespace NES {

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class ValueType;
using ValueTypePtr = std::shared_ptr<ValueType>;

/// @brief Representation of a user-defined constant in the NES type system.
class [[nodiscard]] ValueType {
  public:
    [[nodiscard]] inline explicit ValueType(DataTypePtr && type) : dataType(std::move(type)) {}

    virtual ~ValueType() = default;

    /// @brief Checks if two values are equal
    [[nodiscard]] virtual bool isEquals(ValueTypePtr valueType) const noexcept = 0;

    /// @brief Returns a string representation of this value
    [[nodiscard]] virtual std::string toString() const noexcept = 0;

    template<class CheckedType>
    auto as() {
        return dynamic_cast<CheckedType*>(this);
    }

    DataTypePtr const dataType;
};

}// namespace NES

#endif  // NES_INCLUDE_COMMON_VALUETYPES_VALUETYPE_HPP_
