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

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <Common/DataTypes/Numeric.hpp>

namespace NES
{

/**
 * @brief The Integer type represents whole numbers, that is,
 * numbers without fractional components, of various ranges.
 * The internal Integer type is parameterised by its bit size, and its lower and upper bound
 * Integer(bitSize, lowerBound, upperBound)
 */
class Integer final : public Numeric
{
public:
    /**
     * @brief Constructs a new Integer type.
     * @param bits the number of bits in which this type is represented.
     * @param lowerBound the lower bound, which is contained in that integer.
     * @param upperBound the upper bound, which is contained in that integer.
     */
    Integer(int8_t bits, bool isSigned) noexcept : Numeric(bits), isSigned(isSigned) { }

    ~Integer() override = default;

    bool operator==(const DataType& other) const override;

    /**
    * @brief Calculates the joined data type between this data type and the other.
    * If they have no possible joined data type, the coined type is Undefined.
    * For integers, we can join with all numeric data types.
    * @param other data type
    * @return std::shared_ptr<DataType> joined data type
    */
    [[nodiscard]] std::shared_ptr<DataType> join(const DataType& otherDataType) const override;

    [[nodiscard]] std::string toString() const override;

    [[nodiscard]] bool getIsSigned() const;

private:
    bool isSigned;
};

}
