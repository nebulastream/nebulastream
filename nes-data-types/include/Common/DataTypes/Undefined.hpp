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

#include <Common/DataTypes/DataType.hpp>
namespace NES
{

/**
 * @brief The Undefined type represents a type for without any meaning.
 */
class Undefined final : public DataType
{
public:
    ~Undefined() override = default;

    /**
     * @brief Checks if two data types are equal.
     * @param otherDataType
     * @return true if equal types false if unequal
     */
    bool operator==(const DataType& other) const override;

    /**
     * @brief Calculates the joined data type between this data type and the other.
     * If they have no possible joined data type, the coined type is Undefined.
     * @param other data type
     * @return DataTypePtr joined data type
     */
    DataTypePtr join(DataTypePtr otherDataType) override;

    std::string toString() override;
};

}
