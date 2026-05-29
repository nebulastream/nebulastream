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

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <variant>
#include <Interface/Hash/HashFunction.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>

namespace NES::Nautilus::Interface
{

/// BloomFilter sizing passed from the query compiler to the physical operators.
struct BloomFilterParams
{
    uint64_t bitCount = 0;
    uint64_t hashCount = 0;

    /// Bytes to allocate for the bit array, or 0 when the filter is disabled (hashCount == 0). Used to
    /// size the `ChainedHashMap`'s lazily-allocated bit area so disabled maps allocate nothing.
    [[nodiscard]] uint64_t allocationByteCount() const { return hashCount > 0 ? std::ceil(bitCount / 8.0) : 0; }

    /// Optimal Bloom filter sizing from Bose et al., "On the false-positive rate of Bloom filters",
    /// Information Processing Letters 108(4), 2008. https://doi.org/10.1016/j.ipl.2008.05.018
    [[nodiscard]] static BloomFilterParams compute(std::size_t expectedEntries, double fpRate);
};

/// Public-facing BloomFilter handle used by physical operators. Wraps a variant whose alternatives are
/// private nested types (`Impl` for the real filter, `NoOp` for when the filter is disabled). The only
/// way to obtain a `BloomFilterRef` is through `create(...)`; nothing outside this class can name or
/// construct the variant alternatives directly.
///
/// `Impl` uses the Kirsch-Mitzenmacher double-hashing scheme (split hash into lower/upper 32 bits):
/// A. Kirsch, M. Mitzenmacher, "Less Hashing, Same Performance: Building a Better Bloom Filter",
/// Random Structures & Algorithms, 2008. https://doi.org/10.1002/rsa.20208
class BloomFilterRef
{
public:
    static BloomFilterRef create(bool enabled, uint64_t bitCount, uint64_t hashCount, std::unique_ptr<HashFunction> hashFunction);

    /// Convenience factory for a disabled (NoOp) filter: `mightContain` is constant-true and `add` is empty.
    static BloomFilterRef createDisabled();

    [[nodiscard]] const BloomFilterParams& params() const { return bloomFilterParams; }

    void add(const nautilus::val<uint64_t*>& bitsData, const HashFunction::HashValue& hash) const
    {
        std::visit([&](const auto& bloomFilter) { bloomFilter.add(bitsData, hash); }, impl);
    }

    [[nodiscard]] nautilus::val<bool> mightContain(const nautilus::val<uint64_t*>& bitsData, const HashFunction::HashValue& hash) const
    {
        return std::visit([&](const auto& bloomFilter) { return bloomFilter.mightContain(bitsData, hash); }, impl);
    }

    /// Zeroes `dst`. No-op when the filter is disabled.
    void clearBits(const nautilus::val<std::byte*>& dst) const
    {
        std::visit([&](const auto& bloomFilter) { bloomFilter.clearBits(dst); }, impl);
    }

private:
    /// Real BloomFilter implementation. Method bodies live in BloomFilterRef.cpp.
    class Impl
    {
    public:
        Impl(BloomFilterParams params, std::unique_ptr<HashFunction> hashFunction);
        Impl(const Impl& other);
        Impl(Impl&&) = default;
        Impl& operator=(const Impl& other);
        Impl& operator=(Impl&&) = default;

        void add(const nautilus::val<uint64_t*>& bitsData, const HashFunction::HashValue& hash) const;

        [[nodiscard]] nautilus::val<bool> mightContain(const nautilus::val<uint64_t*>& bitsData, const HashFunction::HashValue& hash) const;

        void clearBits(const nautilus::val<std::byte*>& dst) const;

        [[nodiscard]] const BloomFilterParams& getParams() const { return params; }

    private:
        BloomFilterParams params;
        std::unique_ptr<HashFunction> hashFunction;
    };

    /// Variant used when the filter is disabled — every operation is a trace-level no-op.
    class NoOp
    {
    public:
        void add(const nautilus::val<uint64_t*>&, const HashFunction::HashValue&) const { }

        [[nodiscard]] static nautilus::val<bool> mightContain(const nautilus::val<uint64_t*>&, const HashFunction::HashValue&)
        {
            return nautilus::val<bool>{true};
        }

        void clearBits(const nautilus::val<std::byte*>&) const { }
    };

    explicit BloomFilterRef(Impl impl) : bloomFilterParams{impl.getParams()}, impl{std::move(impl)} { }

    explicit BloomFilterRef(NoOp noop) : impl{std::move(noop)} { }

    BloomFilterParams bloomFilterParams;
    std::variant<Impl, NoOp> impl;
};

}
