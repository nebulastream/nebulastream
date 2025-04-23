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
     * @param nullable whether the integer type is nullable.
     * @param bits the number of bits in which this type is represented.
     * @param isSigned whether the integer type is signed/unsigned.
     */
    Integer(const bool nullable, const int8_t bits, const bool isSigned) noexcept : Numeric(nullable, bits), isSigned(isSigned) { }

    ~Integer() override = default;

    bool operator==(const DataType& other) const override;

    /**
    * @brief Calculates the joined data type between this data type and the other.
    * If they have no possible joined data type, the coined type is Undefined.
    * For integers, we can join with all numeric data types.
    * @param otherDataType data type
    * @return std::shared_ptr<DataType> joined data type
    */
    std::shared_ptr<DataType> join(std::shared_ptr<DataType> otherDataType) override;

    std::string toString() override;

    [[nodiscard]] bool getIsSigned() const;

private:
    bool isSigned;
};

}
