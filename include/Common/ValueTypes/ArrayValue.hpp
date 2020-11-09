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

#ifndef NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
#define NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
#include <Common/ValueTypes/ValueType.hpp>
#include <vector>
namespace NES {

class ArrayValue : public ValueType {
  public:
    ArrayValue(DataTypePtr type, std::vector<std::string> values);
    std::vector<std::string> getValues();

    /**
     * @brief Indicates if this value is a array.
     */
    bool isArrayValue() override;

    /**
    * @brief Returns a string representation of this value
    * @return string
    */
    std::string toString() override;

    /**
     * @brief Checks if two values are equal
     * @param valueType
     * @return bool
     */
    bool isEquals(ValueTypePtr valueType) override;

  private:
    std::vector<std::string> values;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_VALUETYPES_ARRAYVALUETYPE_HPP_
