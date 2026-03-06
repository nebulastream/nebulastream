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
#include <Nautilus/Interface/Hash/BloomFilter.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>

// Forward declaration of existing Murmur-style hash from MurMur3HashFunction.cpp
namespace NES {
std::uint64_t hashBytes(void* data, std::uint64_t length);
}

namespace NES::Nautilus::Interface {

namespace {

// small helper for ceil-division
inline std::size_t divRoundUp(std::size_t x, std::size_t y) {
    return (x + y - 1) / y;
}

} // namespace

BloomFilter::BloomFilter(std::size_t expectedEntries, double falsePositiveRate)
    : bitCount(0)
    , hashCount(0) {

    // Basic sanity checks and defaults
    if (expectedEntries == 0) {
        expectedEntries = 1;
    }
    if (falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
        // default to 1% if user passes an invalid value
        falsePositiveRate = 0.01;
    }

    // Standard Bloom filter formulas:
    //
    //   m = -n * ln(p) / (ln(2)^2)
    //   k = (m / n) * ln(2)
    //
    const double ln2 = std::log(2.0);
    const double mDouble =
        -static_cast<double>(expectedEntries) * std::log(falsePositiveRate) / (ln2 * ln2);

    bitCount = static_cast<std::size_t>(std::ceil(mDouble));
    if (bitCount == 0) {
        bitCount = 1;
    }

    const double kDouble = (bitCount / static_cast<double>(expectedEntries)) * ln2;
    hashCount = static_cast<std::size_t>(std::max(1.0, std::round(kDouble)));

    const std::size_t wordCount = divRoundUp(bitCount, static_cast<std::size_t>(64));
    bits.assign(wordCount, 0);
}

void BloomFilter::clear() {
    std::fill(bits.begin(), bits.end(), 0);
}

void BloomFilter::setBit(std::size_t bitIndex) {
    const std::size_t wordIndex = bitIndex / 64;
    const std::size_t offset    = bitIndex % 64;
    bits[wordIndex] |= (std::uint64_t(1) << offset);
}

bool BloomFilter::getBit(std::size_t bitIndex) const {
    const std::size_t wordIndex = bitIndex / 64;
    const std::size_t offset    = bitIndex % 64;
    return (bits[wordIndex] >> offset) & std::uint64_t(1);
}

void BloomFilter::computeBaseHashes(KeyType key,
                                    std::uint64_t& h1,
                                    std::uint64_t& h2) const {
    // Use existing Murmur-style hash for the bytes of the key.
    // hashBytes takes a non-const void* pointer.
    auto mutableKey = key;
    h1 = ::NES::hashBytes(static_cast<void*>(&mutableKey), static_cast<std::uint64_t>(sizeof(KeyType)));

    // Derive a second hash value from h1 (mixing function similar to robin-hood hashing).
    h2 = h1;
    h2 ^= (h2 >> 33);
    h2 *= 0xff51afd7ed558ccdULL;
    h2 ^= (h2 >> 33);
    h2 *= 0xc4ceb9fe1a85ec53ULL;
    h2 ^= (h2 >> 33);

    // Avoid h2 == 0 to prevent all combined hashes becoming identical.
    if (h2 == 0) {
        h2 = 0x9e3779b97f4a7c15ULL; // arbitrary odd constant
    }
}

void BloomFilter::add(KeyType key) {
    std::uint64_t h1 = 0;
    std::uint64_t h2 = 0;
    computeBaseHashes(key, h1, h2);

    for (std::size_t i = 0; i < hashCount; ++i) {
        const std::uint64_t combined = h1 + i * h2;
        const std::size_t bitIndex =
            static_cast<std::size_t>(combined % static_cast<std::uint64_t>(bitCount));
        setBit(bitIndex);
    }
}

bool BloomFilter::mightContain(KeyType key) const {
    std::uint64_t h1 = 0;
    std::uint64_t h2 = 0;
    computeBaseHashes(key, h1, h2);

    for (std::size_t i = 0; i < hashCount; ++i) {
        const std::uint64_t combined = h1 + i * h2;
        const std::size_t bitIndex =
            static_cast<std::size_t>(combined % static_cast<std::uint64_t>(bitCount));
        if (!getBit(bitIndex)) {
            // one bit is 0 -> key definitely not present (FILTERED)
            return false;
        }
    }
    // all bits are 1 -> key might be present (PASSED - could be false positive)
    return true;
}

} // namespace NES::Nautilus::Interface
