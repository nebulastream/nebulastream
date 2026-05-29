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

/// BloomFilter sizing passed from the query compiler to the physical operators. Every instance is a valid,
/// enabled sizing; the constructor is the single boundary enforcing that. "No filter" is an empty
/// `std::optional` at the holder, so there is no disabled state here and no default constructor.
class BloomFilterParams
{
    uint64_t bitCount;
    uint64_t hashCount;

public:
    /// Bounds the sizing math (which runs in double) so the cast to uint64_t stays defined and the rounding
    /// in `allocationByteCount()` cannot wrap. 2^40 bits is 128 GiB, far past what can be allocated.
    static constexpr uint64_t maxBitCount = 1ULL << 40U;
    /// A filter needs at least one whole word to index into.
    static constexpr uint64_t minBitCount = 64;
    /// `hashCount` unrolls the probe loop at trace time, so it has to stay small. No filter benefits from
    /// more than a few dozen hash positions anyway.
    static constexpr uint64_t minHashCount = 1;
    static constexpr uint64_t maxHashCount = 64;

    /// Optimal sizing from Bose et al., "On the false-positive rate of Bloom filters",
    /// Information Processing Letters 108(4), 2008. https://doi.org/10.1016/j.ipl.2008.05.018
    /// Requires a finite `fpRate` in (0, 1); the derived sizing is clamped into the bounds above.
    [[nodiscard]] BloomFilterParams(std::size_t expectedEntries, double fpRate);

    /// Bytes to allocate for the bit array, sizing the `ChainedHashMap`'s in-map bit area. Rounded up to
    /// whole 64-bit words because `add`/`mightContain` index by word.
    [[nodiscard]] uint64_t allocationByteCount() const { return ((bitCount + 63) / 64) * sizeof(uint64_t); }

    [[nodiscard]] uint64_t getBitCount() const { return bitCount; }

    [[nodiscard]] uint64_t getHashCount() const { return hashCount; }
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
