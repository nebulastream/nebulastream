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
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Nautilus::Interface
{

/**
 * @brief Interface for hash function on Nautilus values.
 * Sub classes can provide specific hash algorithms.
 */
class HashFunction
{
public:
    using HashValue = nautilus::val<uint64_t>;

    /**
     * @brief Calculates the hash of one value.
     * @param value a nautilus value
     * @return the hash
     */
    [[nodiscard]] HashValue calculate(const VarVal& value) const;

    /**
     * @brief Calculates the hash across a set of values.
     * @param values vector of nautilus values.
     * @return the hash
     */
    [[nodiscard]] HashValue calculate(const std::vector<VarVal>& values) const;
    virtual ~HashFunction() = default;

protected:
    /**
     * @brief Initializes a hash value, e.g. a specific seed.
     * @return HashValue
     */
    [[nodiscard]] virtual HashValue init() const = 0;
    /**
     * @brief Calculates the hash of a specific value.
     * @param hash
     * @param value
     * @return HashValue
     */
    virtual HashValue calculate(HashValue& hash, const VarVal& value) const = 0;
};
}
