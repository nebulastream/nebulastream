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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_HASH_H3HASH_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_HASH_H3HASH_HPP_

#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <random>

namespace NES::Nautilus::Interface {
/**
 * @brief H3 implementation taken from the paper Universal classes of hash functions by Carter, J., and Wegman, M. N.
 * Journal of Computer and System Sciences 18, 2 (apr 1979). This class only implements a single row. Thus, Count-Min
 * requires multiple objects to be able to have multiple objects of this class.
 */
class H3Hash : public HashFunction {
  public:
    static constexpr auto H3_SEED = 42;

    H3Hash(uint64_t numberOfKeyBits);

    /**
     * @brief Initializes the hash by just returning zero
     * @return HashValue
     */
    HashValue init() override;

    /**
     * @brief Do not use this method for MurMur3Hash, we require this only until issue #3648 has been fixed TODO
     * @param hash
     * @param value
     * @param state
     * @return HashValue
     */
    HashValue calculate(HashValue& hash, Value<>& value) override;

    /**
     * @brief Calculates the hash for a given value by hashing the value and then xor-ing with hash. This version
     * expects an additional state.
     * @param hash
     * @param value
     * @param state
     * @return HashValue
     */
    HashValue calculateWithState(HashValue& hash, Value<>& value, Value<MemRef>& state) override;

  private:
    uint64_t entrySizeH3HashSeed;
    uint64_t numberOfKeyBits;
};

}// namespace NES::Nautilus::Interface

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_HASH_H3HASH_HPP_
