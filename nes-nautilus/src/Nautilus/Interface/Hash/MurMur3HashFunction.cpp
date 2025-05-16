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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{
HashFunction::HashValue MurMur3HashFunction::init() const
{
    return SEED;
}

std::unique_ptr<HashFunction> MurMur3HashFunction::clone() const
{
    return std::make_unique<MurMur3HashFunction>(*this);
}

/// Hash Function that implements murmurhas3 by Robin-Hood-Hashing:
/// https://github.com/martinus/robin-hood-hashing/blob/fb1483621fda28d4afb31c0097c1a4a457fdd35b/src/include/robin_hood.h#L748
VarVal hashVarVal(const VarVal& input)
{
    /// Define the constants for the hash function.
    constexpr auto murmurHashXorShift = 33;
    constexpr auto murmurHashMultiplier1 = UINT64_C(0xff51afd7ed558ccd);
    constexpr auto murmurHashMultiplier2 = UINT64_C(0xc4ceb9fe1a85ec53);

    /// We are not using the input variable here but rather are creating a new one, as otherwise, the underlying value of the input could change.
    auto hash = input ^ (input >> VarVal(HashFunction::HashValue(murmurHashXorShift)));
    hash = hash * VarVal(nautilus::val<uint64_t>(murmurHashMultiplier1));
    hash = hash ^ (hash >> VarVal(HashFunction::HashValue(murmurHashXorShift)));
    hash = hash * VarVal(nautilus::val<uint64_t>(murmurHashMultiplier2));
    hash = hash ^ (hash >> VarVal(HashFunction::HashValue(murmurHashXorShift)));
    return hash;
}

/**
 * @brief https://github.com/martinus/robin-hood-hashing/blob/fb1483621fda28d4afb31c0097c1a4a457fdd35b/src/include/robin_hood.h#L692
 * @param data
 * @param length
 * @return
 */
uint64_t hashBytes(void* data, uint64_t length)
{
    static constexpr uint64_t m = UINT64_C(0xc6a4a7935bd1e995);
    static constexpr uint64_t seed = UINT64_C(0xe17a1465);
    static constexpr unsigned int r = 47;

    const auto* const data64 = static_cast<const uint64_t*>(data);
    uint64_t h = seed ^ (length * m);

    const size_t nBlocks = length / 8;
    for (size_t i = 0; i < nBlocks; ++i)
    {
        auto k = *(data64 + i);

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const auto* const data8 = reinterpret_cast<const uint8_t*>(data64 + nBlocks);
    switch (length & 7U)
    {
        case 7:
            h ^= static_cast<uint64_t>(data8[6]) << 48U;
        /// FALLTHROUGH
        case 6:
            h ^= static_cast<uint64_t>(data8[5]) << 40U;
        /// FALLTHROUGH
        case 5:
            h ^= static_cast<uint64_t>(data8[4]) << 32U;
        /// FALLTHROUGH
        case 4:
            h ^= static_cast<uint64_t>(data8[3]) << 24U;
        /// FALLTHROUGH
        case 3:
            h ^= static_cast<uint64_t>(data8[2]) << 16U;
        /// FALLTHROUGH
        case 2:
            h ^= static_cast<uint64_t>(data8[1]) << 8U;
        /// FALLTHROUGH
        case 1:
            h ^= static_cast<uint64_t>(data8[0]);
            h *= m;
        /// FALLTHROUGH
        default:
            break;
    }

    h ^= h >> r;

    /// final step
    h *= m;
    h ^= h >> r;
    return h;
}

HashFunction::HashValue MurMur3HashFunction::calculate(HashValue& hash, const VarVal& value) const
{
    return value
        .customVisit(
            [&]<typename T>(const T& val) -> VarVal
            {
                if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    const auto& varSizedContent = val;
                    return hash ^ nautilus::invoke(hashBytes, varSizedContent.getContent(), varSizedContent.getContentSize());
                }
                else if constexpr (std::is_same_v<T, nautilus::val<double>> || std::is_same_v<T, nautilus::val<float>>)
                {
                    /// For floating points, we do not support the >> operator, as it is not defined for floats.
                    throw UnsupportedOperation("Cannot hash floating point values.");
                }
                else
                {
                    return VarVal(hash) ^ hashVarVal(static_cast<nautilus::val<uint64_t>>(val));
                }
            })
        .cast<HashValue>();
}
}
