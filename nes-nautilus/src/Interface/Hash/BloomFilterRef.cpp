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
#include <memory>
#include <numbers>
#include <utility>
#include <Interface/Hash/HashFunction.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{

BloomFilterParams BloomFilterParams::compute(const std::size_t expectedEntries, const double fpRate)
{
    if (expectedEntries == 0)
    {
        return {.bitCount = 64, .hashCount = 1};
    }
    const auto numEntries = static_cast<double>(expectedEntries);
    const double ln2 = std::numbers::ln2;
    const auto numBits = std::max(static_cast<uint64_t>(std::ceil(-numEntries * std::log(fpRate) / (ln2 * ln2))), uint64_t{64});
    const auto numHashes = std::max(static_cast<uint64_t>(std::round(static_cast<double>(numBits) / numEntries * ln2)), uint64_t{1});
    return {.bitCount = numBits, .hashCount = numHashes};
}

BloomFilterRef
BloomFilterRef::create(const bool enabled, const uint64_t bitCount, const uint64_t hashCount, std::unique_ptr<HashFunction> hashFunction)
{
    if (enabled)
    {
        return BloomFilterRef{Impl{BloomFilterParams{.bitCount = bitCount, .hashCount = hashCount}, std::move(hashFunction)}};
    }
    return BloomFilterRef{NoOp{}};
}

BloomFilterRef BloomFilterRef::createDisabled()
{
    return BloomFilterRef{NoOp{}};
}

BloomFilterRef::Impl::Impl(const BloomFilterParams params, std::unique_ptr<HashFunction> hashFunction)
    : params{params}, hashFunction{std::move(hashFunction)}
{
}

BloomFilterRef::Impl::Impl(const Impl& other)
    : params{other.params}, hashFunction{other.hashFunction ? other.hashFunction->clone() : nullptr}
{
}

BloomFilterRef::Impl& BloomFilterRef::Impl::operator=(const Impl& other)
{
    if (this != &other)
    {
        params = other.params;
        hashFunction = other.hashFunction ? other.hashFunction->clone() : nullptr;
    }
    return *this;
}

void BloomFilterRef::Impl::add(const nautilus::val<uint64_t*>& bitsData, const HashFunction::HashValue& hash) const
{
    nautilus::val<uint64_t> lower32 = hash & nautilus::val<uint64_t>{(1ULL << 32) - 1};
    nautilus::val<uint64_t> upper32 = hash >> nautilus::val<uint64_t>{32ULL};

    /// Each iteration computes a derived hash position via h_i(x) = (lower32 + i * upper32) mod bitCount.
    /// The position is split into a word index (which uint64_t in the array) and a bit offset within that word.
    /// Iterating hashCount times sets multiple independent bits, reducing the false-positive rate.
    for (nautilus::val<uint64_t> i{0}; i < params.hashCount; i = i + nautilus::val<uint64_t>{1})
    {
        nautilus::val<uint64_t> x = (lower32 + i * upper32) % params.bitCount;
        const nautilus::val<uint64_t> wordIndex = x / nautilus::val<uint64_t>{64ULL};
        nautilus::val<uint64_t> bitOffset = x % nautilus::val<uint64_t>{64ULL};
        const nautilus::val<uint64_t> mask = nautilus::val<uint64_t>{1ULL} << bitOffset;
        *(bitsData + wordIndex) = *(bitsData + wordIndex) | mask;
    }
}

nautilus::val<bool> BloomFilterRef::Impl::mightContain(const nautilus::val<uint64_t*>& bitsData, const HashFunction::HashValue& hash) const
{
    nautilus::val<uint64_t> lower32 = hash & nautilus::val<uint64_t>{(1ULL << 32) - 1};
    nautilus::val<uint64_t> upper32 = hash >> nautilus::val<uint64_t>{32ULL};

    nautilus::val<uint64_t> allPresent{1ULL};
    for (nautilus::val<uint64_t> i{0}; i < params.hashCount; i = i + nautilus::val<uint64_t>{1})
    {
        nautilus::val<uint64_t> x = (lower32 + i * upper32) % params.bitCount;
        const nautilus::val<uint64_t> wordIndex = x / nautilus::val<uint64_t>{64ULL};
        nautilus::val<uint64_t> bitOffset = x % nautilus::val<uint64_t>{64ULL};
        nautilus::val<uint64_t> word = *(bitsData + wordIndex);
        nautilus::val<uint64_t> bit = (word >> bitOffset) & nautilus::val<uint64_t>{1ULL};
        allPresent = allPresent & bit;
    }
    return allPresent != nautilus::val<uint64_t>{0ULL};
}

void BloomFilterRef::Impl::clearBits(const nautilus::val<std::byte*>& dst) const
{
    const auto wordCount = params.allocationByteCount() / sizeof(uint64_t);
    auto dstWords = static_cast<nautilus::val<uint64_t*>>(dst);
    for (nautilus::val<uint64_t> i{0}; i < wordCount; i = i + nautilus::val<uint64_t>{1})
    {
        *(dstWords + i) = nautilus::val<uint64_t>{0};
    }
}

}
