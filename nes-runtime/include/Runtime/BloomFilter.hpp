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
#ifndef NES_RUNTIME_INCLUDE_RUNTIME_BLOOMFILTER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_BLOOMFILTER_HPP_

#include <cstddef>
#include <cstdint>
#include <cstdlib>

namespace NES::Runtime {

/**
 * @brief A bloom filter that works with 64-bit keys
 */
class alignas(64) BloomFilter {

  public:
    /**
     * @brief Creates a bloom filter for the expected no. entries and the false positive rate
     * @param entries
     * @param falsePositiveRate
     */
    explicit BloomFilter(uint64_t entries, double falsePositiveRate);

    /**
     * @brief adds a hash to the bloom filter
     * @param hash
     */
    void add(uint64_t hash);

    /**
     * @brief checks if the key is in the bloom filter. If false, one can be sure that the key has not been seen
     * @param hash
     * @return true or false
     */
    bool checkContains(uint64_t hash);

    /**
     * @brief destructs the BloomFilter
     */
    ~BloomFilter();

  private:
    uint32_t noBits;
    uint16_t noHashes;
    uint8_t* bitField;
};

}// namespace NES::Runtime
#endif// NES_RUNTIME_INCLUDE_RUNTIME_BLOOMFILTER_HPP_
