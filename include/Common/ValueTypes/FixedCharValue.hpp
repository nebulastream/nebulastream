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

#ifndef NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_

#include <Common/ValueTypes/ValueType.hpp>
#include <string>
#include <vector>

namespace NES {

/**
 * @brief Represents a FixedCharValue. This can ether constructed from a vector of string values, or a string.
 */
class FixedCharValue : public ValueType {
  public:
    FixedCharValue(std::vector<std::string> values);
    FixedCharValue(const std::string& value);

    /**
     * @brief Checks if two values are equal
     * @param valueType
     * @return bool
     */
    bool isEquals(ValueTypePtr valueType) override;

    /**
    * @brief Returns a string representation of this value
    * @return string
    */
    std::string toString() override;

    /**
     * @brief Indicates if this value is a char value.
     */
    bool isCharValue() override;

    /**
     * @brief Returns the values represented by the fixed char.
     * @return
     */
    const std::vector<std::string>& getValues() const;

    /**
     * @brief Indicates if this value is a string
     * @return
     */
    bool getIsString() const;

  private:
    std::vector<std::string> values;
    bool isString;
};
}// namespace NES

#endif//NES_INCLUDE_DATATYPES_VALUETYPES_CHARVALUETYPE_HPP_
