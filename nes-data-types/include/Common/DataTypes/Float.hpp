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

#include <memory>
#include <string>
#include <Common/DataTypes/Numeric.hpp>

namespace NES
{

/**
 * @brief Float precision are inexact, variable-precision numeric types.
 * Inexact means that some values cannot be converted exactly to the internal format and are stored as approximations,
 * so that storing and retrieving a value might show slight discrepancies.
 */
class Float final : public Numeric
{
public:
    /**
     * @brief Constructs a new Float type.
     * @param bits the number of bits in which this type is represented.
     * @param lowerBound the lower bound, which is contained in that float.
     * @param upperBound the upper bound, which is contained in that float.
     */
    explicit Float(int8_t bits) noexcept : Numeric(bits) { }

    ~Float() override = default;

    bool operator==(const DataType& other) const override;

    /**
    * @brief Calculates the joined data type between this data type and the other.
    * If they have no possible joined data type, the coined type is Undefined.
    * Floats, we can join with all numeric data types.
    * @param other data type
    * @return std::shared_ptr<DataType> joined data type
    */
    [[nodiscard]] std::shared_ptr<DataType> join(const DataType& otherDataType) const override;

    [[nodiscard]] std::string toString() const override;
};
}
