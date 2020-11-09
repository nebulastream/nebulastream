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

#ifndef NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_

#include "ValueType.hpp"

namespace NES {

class BasicValue : public ValueType {

  public:
    BasicValue(DataTypePtr type, std::string value);

    /**
    * @brief Indicates if this value is a basic value.
    */
    bool isBasicValue() override;

    /**
    * @brief Returns a string representation of this value
    * @return string
    */
    std::string toString() override;

    /**
     * @brief Get the value of this value type.
     * @return string
     */
    std::string getValue();

    /**
     * @brief Checks if two values are equal
     * @param valueType
     * @return bool
     */
    bool isEquals(ValueTypePtr valueType) override;

  private:
    std::string value;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_BASICVALUETYPE_HPP_
