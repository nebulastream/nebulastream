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

#include <cstddef>
#include <cstdint>
#include <Interface/Hash/HashFunction.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{

/// BloomFilter sizing passed from the query compiler to the physical operators.
struct BloomFilterParams
{
    uint64_t bitCount = 0;
    uint64_t hashCount = 0;

    /// Bytes to allocate for the bit array, or 0 when the filter is disabled (hashCount == 0). Used to
    /// size the `ChainedHashMap`'s in-map bit area so disabled maps allocate nothing.
    /// Rounded up to whole 64-bit words: `add`/`mightContain` index by word (`bitCount - 1` may land in the
    /// last, partially used word), so a byte-exact size would leave that word's tail out of bounds.
    [[nodiscard]] uint64_t allocationByteCount() const
    {
        if (hashCount == 0)
        {
            return 0;
        }
        return ((bitCount + 63) / 64) * sizeof(uint64_t);
    }

    /// Optimal Bloom filter sizing from Bose et al., "On the false-positive rate of Bloom filters",
    /// Information Processing Letters 108(4), 2008. https://doi.org/10.1016/j.ipl.2008.05.018
    [[nodiscard]] static BloomFilterParams compute(std::size_t expectedEntries, double fpRate);
};

/// Public-facing BloomFilter handle used by physical operators. Every instance is a real filter: "no filter"
/// is expressed by the holder as an empty `std::optional<BloomFilterRef>`, not by an alternative in here.
///
/// The bit area is a runtime address, the sizing is trace-time data: hashCount unrolls the probe loop and
/// bitCount folds into the modulo. Params therefore have to match the ones the bit area was allocated with.
///
/// Uses the Kirsch-Mitzenmacher double-hashing scheme (split hash into lower/upper 32 bits):
/// A. Kirsch, M. Mitzenmacher, "Less Hashing, Same Performance: Building a Better Bloom Filter",
/// Random Structures & Algorithms, 2008. https://doi.org/10.1002/rsa.20208
class BloomFilterRef
{
public:
    BloomFilterRef(const nautilus::val<uint64_t*>& memArea, BloomFilterParams params);

    void add(const HashFunction::HashValue& hash) const;

    /// Checks if a particular hash might be seen by the bloom filter
    [[nodiscard]] nautilus::val<bool> mightContain(const HashFunction::HashValue& hash) const;

private:
    nautilus::val<uint64_t*> memArea;
    BloomFilterParams params;
};

}
