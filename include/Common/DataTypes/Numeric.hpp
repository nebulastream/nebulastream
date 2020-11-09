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

#ifndef NES_INCLUDE_DATATYPES_NUMERIC_HPP_
#define NES_INCLUDE_DATATYPES_NUMERIC_HPP_
#include <Common/DataTypes/DataType.hpp>
namespace NES {

/**
 * @brief The Numeric type represents integers and floats.
 */
class Numeric : public DataType {
  public:
    explicit Numeric(int8_t bits);

    /**
    * @brief Checks if this data type is Numeric.
    * @return bool
    */
    bool isNumeric() override;

    /**
     * @brief Gets the bit size of this type.
     * @return int8_t
     */
    [[nodiscard]] int8_t getBits() const;

    /**
    * @brief Returns a string representation of the data type.
    * @return string
    */
    virtual std::string toString() override = 0;

  protected:
    const int8_t bits;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_NUMERIC_HPP_
