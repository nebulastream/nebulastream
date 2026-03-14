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
#include <Nautilus/Interface/Hash/SipHashFunction.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <nautilus/function.hpp>
#include <nautilus/val.hpp>
#include <ErrorHandling.hpp>
#include <HashFunctionRegistry.hpp>

namespace NES
{

/// Fixed default key for SipHash-2-4 (two 64-bit halves: k0, k1).
static constexpr uint64_t SIPHASH_DEFAULT_K0 = UINT64_C(0x0706050403020100);
static constexpr uint64_t SIPHASH_DEFAULT_K1 = UINT64_C(0x0f0e0d0c0b0a0908);

/// SipHash-2-4 initialization constant derived from the seed.
static constexpr uint64_t SIPHASH_SEED = UINT64_C(0x736970686173685f);

HashFunction::HashValue SipHashFunction::init() const
{
    return SIPHASH_SEED;
}

std::unique_ptr<HashFunction> SipHashFunction::clone() const
{
    return std::make_unique<SipHashFunction>(*this);
}

namespace
{

/// Rotate left helper for 64-bit values.
inline uint64_t rotl64(uint64_t x, int b)
{
    return (x << b) | (x >> (64 - b));
}

/// One SipRound: mixes the four state variables v0-v3.
inline void sipRound(uint64_t& v0, uint64_t& v1, uint64_t& v2, uint64_t& v3)
{
    v0 += v1;
    v1 = rotl64(v1, 13);
    v1 ^= v0;
    v0 = rotl64(v0, 32);
    v2 += v3;
    v3 = rotl64(v3, 16);
    v3 ^= v2;
    v0 += v3;
    v3 = rotl64(v3, 21);
    v3 ^= v0;
    v2 += v1;
    v1 = rotl64(v1, 17);
    v1 ^= v2;
    v2 = rotl64(v2, 32);
}

/// Full SipHash-2-4 on a byte buffer. Returns a 64-bit hash.
uint64_t sipHash24(void* data, uint64_t length)
{
    const auto k0 = SIPHASH_DEFAULT_K0;
    const auto k1 = SIPHASH_DEFAULT_K1;

    /// Initialize state.
    uint64_t v0 = k0 ^ UINT64_C(0x736f6d6570736575);
    uint64_t v1 = k1 ^ UINT64_C(0x646f72616e646f6d);
    uint64_t v2 = k0 ^ UINT64_C(0x6c7967656e657261);
    uint64_t v3 = k1 ^ UINT64_C(0x7465646279746573);

    const auto* ptr = static_cast<const uint8_t*>(data);
    const auto nBlocks = length / 8;

    /// Compression: process 8-byte blocks with 2 SipRounds each.
    for (uint64_t i = 0; i < nBlocks; ++i)
    {
        uint64_t m = 0;
        std::memcpy(&m, ptr + i * 8, 8);
        v3 ^= m;
        sipRound(v0, v1, v2, v3);
        sipRound(v0, v1, v2, v3);
        v0 ^= m;
    }

    /// Pack remaining bytes into the last word with length in the high byte.
    uint64_t last = static_cast<uint64_t>(length & 0xff) << 56;
    const auto* tail = ptr + nBlocks * 8;
    const auto remaining = length & 7;

    switch (remaining)
    {
        case 7:
            last |= static_cast<uint64_t>(tail[6]) << 48;
            [[fallthrough]];
        case 6:
            last |= static_cast<uint64_t>(tail[5]) << 40;
            [[fallthrough]];
        case 5:
            last |= static_cast<uint64_t>(tail[4]) << 32;
            [[fallthrough]];
        case 4:
            last |= static_cast<uint64_t>(tail[3]) << 24;
            [[fallthrough]];
        case 3:
            last |= static_cast<uint64_t>(tail[2]) << 16;
            [[fallthrough]];
        case 2:
            last |= static_cast<uint64_t>(tail[1]) << 8;
            [[fallthrough]];
        case 1:
            last |= static_cast<uint64_t>(tail[0]);
            [[fallthrough]];
        default:
            break;
    }

    v3 ^= last;
    sipRound(v0, v1, v2, v3);
    sipRound(v0, v1, v2, v3);
    v0 ^= last;

    /// Finalization: 4 SipRounds.
    v2 ^= 0xff;
    sipRound(v0, v1, v2, v3);
    sipRound(v0, v1, v2, v3);
    sipRound(v0, v1, v2, v3);
    sipRound(v0, v1, v2, v3);

    return v0 ^ v1 ^ v2 ^ v3;
}

/// SipHash-2-4 on a single 64-bit value (treated as 8 bytes).
VarVal sipHashVarVal(const VarVal& input)
{
    const auto k0 = SIPHASH_DEFAULT_K0;
    const auto k1 = SIPHASH_DEFAULT_K1;

    /// Initialize state.
    auto v0 = nautilus::val<uint64_t>(k0 ^ UINT64_C(0x736f6d6570736575));
    auto v1 = nautilus::val<uint64_t>(k1 ^ UINT64_C(0x646f72616e646f6d));
    auto v2 = nautilus::val<uint64_t>(k0 ^ UINT64_C(0x6c7967656e657261));
    auto v3 = nautilus::val<uint64_t>(k1 ^ UINT64_C(0x7465646279746573));

    /// The input value treated as a single 8-byte message block.
    auto m = input.getRawValueAs<nautilus::val<uint64_t>>();

    /// Compression: 2 SipRounds.
    v3 = v3 ^ m;
    /// SipRound 1
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));
    /// SipRound 2
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));
    v0 = v0 ^ m;

    /// Last block: length byte in high position (length = 8, so 0x08 << 56).
    auto last = nautilus::val<uint64_t>(UINT64_C(0x0800000000000000));
    v3 = v3 ^ last;
    /// SipRound 1
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));
    /// SipRound 2
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));
    v0 = v0 ^ last;

    /// Finalization: XOR 0xff into v2, then 4 SipRounds.
    v2 = v2 ^ nautilus::val<uint64_t>(UINT64_C(0xff));
    /// SipRound 1
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));
    /// SipRound 2
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));
    /// SipRound 3
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));
    /// SipRound 4
    v0 = v0 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(13)) | (v1 >> nautilus::val<uint64_t>(51));
    v1 = v1 ^ v0;
    v0 = (v0 << nautilus::val<uint64_t>(32)) | (v0 >> nautilus::val<uint64_t>(32));
    v2 = v2 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(16)) | (v3 >> nautilus::val<uint64_t>(48));
    v3 = v3 ^ v2;
    v0 = v0 + v3;
    v3 = (v3 << nautilus::val<uint64_t>(21)) | (v3 >> nautilus::val<uint64_t>(43));
    v3 = v3 ^ v0;
    v2 = v2 + v1;
    v1 = (v1 << nautilus::val<uint64_t>(17)) | (v1 >> nautilus::val<uint64_t>(47));
    v1 = v1 ^ v2;
    v2 = (v2 << nautilus::val<uint64_t>(32)) | (v2 >> nautilus::val<uint64_t>(32));

    return VarVal(v0 ^ v1 ^ v2 ^ v3);
}

} /// anonymous namespace

HashFunction::HashValue SipHashFunction::calculate(HashValue& hash, const VarVal& value) const
{
    return value
        .customVisit(
            [&]<typename T>(const T& val) -> VarVal
            {
                if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    const auto& varSizedContent = val;
                    return hash ^ nautilus::invoke(sipHash24, varSizedContent.getContent(), varSizedContent.getSize());
                }
                else
                {
                    return VarVal{hash} ^ sipHashVarVal(static_cast<nautilus::val<uint64_t>>(val));
                }
            })
        .getRawValueAs<HashValue>();
}

HashFunctionRegistryReturnType HashFunctionGeneratedRegistrar::RegisterSipHashHashFunction(HashFunctionRegistryArguments)
{
    return std::make_unique<SipHashFunction>();
}
}
