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

#include <Interface/Hash/BloomFilterRef.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <numbers>
#include <Interface/Hash/HashFunction.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{

namespace
{
/// Upper bound on the derived bit count. The sizing math runs in double, where a large `expectedEntries`
/// paired with a small `fpRate` easily exceeds uint64_t: the cast would then be undefined and
/// `allocationByteCount()` would wrap into an under-sized allocation. 2^40 bits is 128 GiB of filter,
/// far beyond anything that can be allocated, so clamping there costs nothing real.
constexpr double MaxBitCount = 1ULL << 40U;
constexpr double MinBitCount = 64;
}

BloomFilterParams BloomFilterParams::compute(const std::size_t expectedEntries, const double fpRate)
{
    if (expectedEntries == 0)
    {
        return {.bitCount = 64, .hashCount = 1};
    }
    const auto numEntries = static_cast<double>(expectedEntries);
    const double ln2 = std::numbers::ln2;
    /// Clamped in double space, before the cast, so an out-of-range result never reaches uint64_t.
    const auto numBits
        = static_cast<uint64_t>(std::clamp(std::ceil(-numEntries * std::log(fpRate) / (ln2 * ln2)), MinBitCount, MaxBitCount));
    const auto numHashes = std::max(static_cast<uint64_t>(std::round(static_cast<double>(numBits) / numEntries * ln2)), uint64_t{1});
    return {.bitCount = numBits, .hashCount = numHashes};
}

BloomFilterRef::BloomFilterRef(const nautilus::val<uint64_t*>& memArea, const BloomFilterParams params) : memArea{memArea}, params{params}
{
}

void BloomFilterRef::add(const HashFunction::HashValue& hash) const
{
    const nautilus::val<uint64_t> lower32 = hash & nautilus::val<uint64_t>{(1ULL << 32) - 1};
    const nautilus::val<uint64_t> upper32 = hash >> nautilus::val<uint64_t>{32ULL};

    /// Each iteration computes a derived hash position via h_i(x) = (lower32 + i * upper32) mod bitCount.
    /// The position is split into a word index (which uint64_t in the array) and a bit offset within that word.
    /// Iterating hashCount times sets multiple independent bits, reducing the false-positive rate.
    for (nautilus::static_val<uint64_t> i{0}; i < params.hashCount; ++i)
    {
        const nautilus::val<uint64_t> x = (lower32 + (i * upper32)) % params.bitCount;
        const nautilus::val<uint64_t> wordIndex = x / nautilus::val<uint64_t>{64ULL};
        const nautilus::val<uint64_t> bitOffset = x % nautilus::val<uint64_t>{64ULL};
        const nautilus::val<uint64_t> mask = nautilus::val<uint64_t>{1ULL} << bitOffset;
        *(memArea + wordIndex) = *(memArea + wordIndex) | mask;
    }
}

nautilus::val<bool> BloomFilterRef::mightContain(const HashFunction::HashValue& hash) const
{
    const nautilus::val<uint64_t> lower32 = hash & nautilus::val<uint64_t>{(1ULL << 32) - 1};
    const nautilus::val<uint64_t> upper32 = hash >> nautilus::val<uint64_t>{32ULL};

    nautilus::val<uint64_t> allPresent{1ULL};
    for (nautilus::static_val<uint64_t> i{0}; i < params.hashCount; ++i)
    {
        const nautilus::val<uint64_t> x = (lower32 + (i * upper32)) % params.bitCount;
        const nautilus::val<uint64_t> wordIndex = x / nautilus::val<uint64_t>{64ULL};
        const nautilus::val<uint64_t> bitOffset = x % nautilus::val<uint64_t>{64ULL};
        const nautilus::val<uint64_t> word = *(memArea + wordIndex);
        const nautilus::val<uint64_t> bit = (word >> bitOffset) & nautilus::val<uint64_t>{1ULL};
        allPresent = allPresent & bit;
    }
    return allPresent != nautilus::val<uint64_t>{0ULL};
}

}
