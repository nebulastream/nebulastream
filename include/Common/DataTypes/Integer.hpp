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

#ifndef NES_INCLUDE_DATATYPES_INTEGER_HPP_
#define NES_INCLUDE_DATATYPES_INTEGER_HPP_

#include <Common/DataTypes/Numeric.hpp>

namespace NES {

/**
 * @brief The Integer type represents whole numbers, that is,
 * numbers without fractional components, of various ranges.
 * The internal Integer type is parameterised by its bit size, and its lower and upper bound
 * Integer(bitSize, lowerBound, upperBound)
 */
class Integer : public Numeric {

  public:
    /**
     * @brief Constructs a new Integer type.
     * @param bits the number of bits in which this type is represented.
     * @param lowerBound the lower bound, which is contained in that integer.
     * @param upperBound the upper bound, which is contained in that integer.
     */
    Integer(int8_t bits, int64_t lowerBound, int64_t upperBound);

    /**
    * @brief Checks if this data type is Integer.
    */
    bool isInteger() override;

    /**
    * @brief Checks if two data types are equal.
    * @param otherDataType
    * @return
    */
    bool isEquals(DataTypePtr otherDataType) override;

    /**
    * @brief Calculates the joined data type between this data type and the other.
    * If they have no possible joined data type, the coined type is Undefined.
    * For integers, we can join with all numeric data types.
    * @param other data type
    * @return DataTypePtr joined data type
    */
    DataTypePtr join(DataTypePtr otherDataType) override;

    /**
     * @brief Returns the lower bound of this integer.
     * @return lowerBound
     */
    [[nodiscard]] int64_t getLowerBound() const;

    /**
     * @brief Returns the upper bound of this integer.
     * @return upperBound
     */
    [[nodiscard]] int64_t getUpperBound() const;

    /**
    * @brief Returns a string representation of the data type.
    * @return string
    */
    std::string toString() override;

  private:
    const int64_t lowerBound;
    const int64_t upperBound;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_INTEGER_HPP_
