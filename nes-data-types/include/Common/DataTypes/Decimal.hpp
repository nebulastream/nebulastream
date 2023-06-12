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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_DECIMAL_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_DECIMAL_HPP_
#include <Common/DataTypes/DataType.hpp>
namespace NES {

/**
 * @brief The Decimal data type represents an exact fixed-point decimal value.
 * When creating a value of type DECIMAL, the SCALE can be specified to define which size of decimal values can be held in the field.
 * The scale determines the amount of digits after the decimal point.
 * For example, the type DECIMAL(2) can fit the value 1.23, but cannot fit the value 1.234.
 */
class Decimal : public DataType {
  public:
    explicit Decimal(int8_t scale);
    virtual ~Decimal() = default;
    /**
     * @brief Gets the bit size of this type.
     * @return int8_t
     */
    [[nodiscard]] int8_t getScale() const;
    bool isEquals(DataTypePtr otherDataType) override;
    DataTypePtr join(DataTypePtr otherDataType) override;
    std::string toString() override;
    bool isDecimal() const override;

  protected:
    const int8_t scale;
};

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_DECIMAL_HPP_
