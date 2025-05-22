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
#include <optional>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

/**
 * @brief The Numeric type represents integers and floats.
 */
class Numeric : public DataType
{
public:
    explicit Numeric(int8_t bits);
    ~Numeric() override = default;

    /**
     * @brief Gets the bit size of this type.
     * @return int8_t
     */
    [[nodiscard]] int8_t getBits() const;

    /// Returns the common type of two numeric types. If no conversion exists between the two types, we return an empty optional.
    static std::optional<std::shared_ptr<DataType>> inferDataType(const Numeric& left, const Numeric& right);

protected:
    const int8_t bits;
};

}
