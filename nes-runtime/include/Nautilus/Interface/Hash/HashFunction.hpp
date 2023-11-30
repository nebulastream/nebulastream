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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_HASH_HASHFUNCTION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_HASH_HASHFUNCTION_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Nautilus::Interface {

/**
 * @brief Interface for hash function on Nautilus values.
 * Sub classes can provide specific hash algorithms.
 */
class HashFunction {
  public:
    using HashValue = Value<UInt64>;

    /**
     * @brief Calculates the hash of one value.
     * @param value a nautilus value
     * @return the hash
     */
    HashValue calculate(Value<> value);

    /**
     * @brief This is only necessary as long as TODO #3648 is not merged
     * @param value
     * @param state
     * @return HashValue
     */
    HashValue calculateWithState(Value<> value, Value<MemRef> state);

    /**
     * @brief Calculates the hash across a set of values.
     * @param values vector of nautilus values.
     * @return the hash
     */
    HashValue calculate(std::vector<Value<>>& values);
    virtual ~HashFunction() = default;

  protected:
    /**
     * @brief Initializes a hash value, e.g. a specific seed.
     * @return HashValue
     */
    virtual HashValue init() = 0;
    /**
     * @brief Calculates the hash of a specific value.
     * @param hash
     * @param value
     * @return HashValue
     */
    virtual HashValue calculate(HashValue& hash, Value<>& value) = 0;

    /**
     * @brief This is only necessary as long as TODO #3648 is not merged
     * @param value
     * @param state
     * @return HashValue
     */
    virtual HashValue calculateWithState(HashValue& hash, Value<>& value, Value<MemRef>& state) = 0;
};
}// namespace NES::Nautilus::Interface
#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_HASH_HASHFUNCTION_HPP_
