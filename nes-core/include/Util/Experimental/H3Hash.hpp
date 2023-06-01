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

#include <Util/Experimental/Hash.hpp>
#include <random>

namespace NES::Experimental {
/**
 * @brief H3 implementation taken from the paper Universal classes of hash functions by Carter, J., and Wegman, M. N.
 * Journal of Computer and System Sciences 18, 2 (apr 1979)
 */
class H3Hash : public Hash<H3Hash> {
  public:
    static constexpr auto H3_SEED = 42;

    H3Hash(const std::vector<hash_t> &h3Seeds) : h3Seeds(h3Seeds) {}
    H3Hash(const uint64_t numberOfRows, const uint64_t numberOfBitsInKey) {
        std::random_device rd;
        std::mt19937 gen(H3_SEED);
        std::uniform_int_distribution<hash_t> distribution;

        for (auto row = 0UL; row < numberOfRows; ++row) {
            for (auto keyBit = 0UL; keyBit < numberOfBitsInKey; ++keyBit) {
                h3Seeds.emplace_back(distribution(gen));
            }
        }
    }

    /**
     * @Brief This functions hashes the key according to h3 hash function for a uint64_t key
     * @param key
     * @param row
     * @return Hash
     */
    inline hash_t hashKey(uint64_t key, hash_t row) const {
        hash_t hash = 0;
        auto numberOfKeyBits = sizeof(uint64_t) * 8;
        for (auto i = 0UL; i < numberOfKeyBits; ++i) {
            bool isBitSet = (key >> i) & 1;
            if (isBitSet) {
                hash = hash ^ h3Seeds[row * numberOfKeyBits + i];
            }
        }

        return hash;
    }

    /**
     * @Brief This functions hashes the key according to h3 hash function for a uint64_t key
     * @param key
     * @param row
     * @return Hash
     */
    inline hash_t hashKey(uint32_t key, hash_t row) const {
        uint32_t hash = 0;
        auto numberOfKeyBits = sizeof(uint32_t) * 8;
        for (auto i = 0UL; i < numberOfKeyBits; ++i) {
            bool isBitSet = (key >> i) & 1;
            if (isBitSet) {
                hash = hash ^ (uint32_t) h3Seeds[row * numberOfKeyBits + i];
            }
        }

        return hash;
    }

private:
    std::vector<hash_t> h3Seeds;
};

} // namespace NES::Experimental

#endif //NES_H3HASH_HPP
