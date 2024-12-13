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
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <nautilus/function.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{
HashFunction::HashValue MurMur3HashFunction::init()
{
    return SEED;
}

/// Hash Function that implements murmurhas3 by Robin-Hood-Hashing:
/// https://github.com/martinus/robin-hood-hashing/blob/fb1483621fda28d4afb31c0097c1a4a457fdd35b/src/include/robin_hood.h#L748
VarVal hashVarVal(const VarVal& input)
{
    /// We are not using the input variable here but rather are creating a new one, as otherwise, the underlying value of the input could change.
    auto x = input ^ (input >> VarVal(33));
    x = x * VarVal(nautilus::val<uint64_t>(UINT64_C(0xff51afd7ed558ccd)));
    x = x ^ (x >> VarVal(33));
    x = x * VarVal(nautilus::val<uint64_t>(UINT64_C(0xc4ceb9fe1a85ec53)));
    x = x ^ (x >> VarVal(33));
    return x;
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

    auto const* const data64 = static_cast<const uint64_t*>(data);
    uint64_t h = seed ^ (length * m);

    size_t const n_blocks = length / 8;
    for (size_t i = 0; i < n_blocks; ++i)
    {
        auto k = *(data64 + i);

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    auto const* const data8 = reinterpret_cast<uint8_t const*>(data64 + n_blocks);
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

HashFunction::HashValue MurMur3HashFunction::calculate(HashValue& hash, VarVal& value)
{
    return value
        .customVisit(
            [&]<typename T>(const T& val) -> VarVal
            {
                if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    const auto& varSizedContent = val;
                    return hash ^ nautilus::invoke(hashBytes, varSizedContent.getContent(), varSizedContent.getSize());
                }
                else
                {
                    return VarVal(hash) ^ hashVarVal(val);
                }
            })
        .cast<HashValue>();
}
}
