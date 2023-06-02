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

#ifndef NES_H3HASH_HPP
#define NES_H3HASH_HPP

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
    /**
     * @brief Creates an object of this class. Expects the seeds and the number of bits in the key
     * @param h3Seeds
     * @param numberOfKeyBits
     */
    explicit H3Hash(const std::vector<uint64_t>& h3Seeds, uint64_t numberOfKeyBits);

    /**
     * @brief Initis the hash by just returning zero
     * @return HashValue
     */
    HashValue init() override;

    /**
     * @brief Calculates the hash for a given key by hashing the key and then xoring with hash
     * @param hash
     * @param value
     * @return HashValue
     */
    HashValue calculate(HashValue &hash, Value<> &value) override;

private:
    /**
     * @brief Gets a bitmask for a given number of bits, for example 8 --> 0xFF and 32 -> 0xFFFF
     * @param numberOfKeyBits
     * @return uint64_t
     */
    uint64_t getBitMask(uint64_t numberOfKeyBits) const;

    std::vector<uint64_t> h3Seeds;
};

} // namespace NES::Nautilus::Interface

#endif //NES_H3HASH_HPP
