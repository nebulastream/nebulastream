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
#include <Nautilus/Interface/Hash/XXH3HashFunction.hpp>

#include <cstdint>
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

/// XXH3-64 constants based on the xxHash algorithm by Yann Collet (public domain / BSD 2-Clause).
/// See: https://github.com/Cyan4973/xxHash
namespace XXH3Constants
{
/// Prime constants used in mixing and avalanche steps.
constexpr uint64_t PRIME64_1 = UINT64_C(0x9E3779B185EBCA87);
constexpr uint64_t PRIME64_2 = UINT64_C(0xC2B2AE3D27D4EB4F);
constexpr uint64_t PRIME64_3 = UINT64_C(0x165667B19E3779F9);
constexpr uint64_t PRIME64_4 = UINT64_C(0x85EBCA77C2B2AE63);
constexpr uint64_t PRIME64_5 = UINT64_C(0x27D4EB2F165667C5);

/// The first 192 bytes of the default XXH3 secret, used for key-dependent mixing.
constexpr uint8_t DEFAULT_SECRET[192] = {
    0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c, 0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90,
    0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f, 0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d,
    0xcc, 0xff, 0x72, 0x21, 0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c, 0x3c, 0x28,
    0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3, 0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e,
    0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8, 0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b,
    0x4f, 0x1d, 0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64, 0xea, 0xc5, 0xac, 0x83,
    0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb, 0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16,
    0xca, 0xce, 0x09, 0xb6, 0x4c, 0x39, 0x9c, 0xfe, 0xb0, 0xa0, 0x32, 0x60, 0xaa, 0xc5, 0xbf, 0xec, 0x3c, 0x6f, 0x09, 0x67, 0xaa, 0x82,
    0xd7, 0x65, 0x53, 0x1c, 0x15, 0xbc, 0x53, 0x6a, 0xf4, 0x51, 0xc6, 0xa0, 0x6e, 0x6a, 0xf0, 0x59,
};

/// Reads a 32-bit value from an unaligned byte pointer in little-endian order.
constexpr uint32_t readLE32(const uint8_t* ptr)
{
    return static_cast<uint32_t>(ptr[0]) | (static_cast<uint32_t>(ptr[1]) << 8) | (static_cast<uint32_t>(ptr[2]) << 16)
        | (static_cast<uint32_t>(ptr[3]) << 24);
}

/// Reads a 64-bit value from an unaligned byte pointer in little-endian order.
constexpr uint64_t readLE64(const uint8_t* ptr)
{
    return static_cast<uint64_t>(ptr[0]) | (static_cast<uint64_t>(ptr[1]) << 8) | (static_cast<uint64_t>(ptr[2]) << 16)
        | (static_cast<uint64_t>(ptr[3]) << 24) | (static_cast<uint64_t>(ptr[4]) << 32) | (static_cast<uint64_t>(ptr[5]) << 40)
        | (static_cast<uint64_t>(ptr[6]) << 48) | (static_cast<uint64_t>(ptr[7]) << 56);
}

/// 128-bit multiply: returns the xor of the high and low 64-bit halves of a * b.
inline uint64_t mul128Fold64(uint64_t a, uint64_t b)
{
    const auto aLo = static_cast<uint64_t>(static_cast<uint32_t>(a));
    const auto aHi = a >> 32;
    const auto bLo = static_cast<uint64_t>(static_cast<uint32_t>(b));
    const auto bHi = b >> 32;

    const auto loLo = aLo * bLo;
    const auto hiLo = aHi * bLo;
    const auto loHi = aLo * bHi;
    const auto hiHi = aHi * bHi;

    const auto cross = (loLo >> 32) + static_cast<uint64_t>(static_cast<uint32_t>(hiLo)) + loHi;
    const auto upper = (cross >> 32) + (hiLo >> 32) + hiHi;
    const auto lower = (cross << 32) | static_cast<uint64_t>(static_cast<uint32_t>(loLo));

    return upper ^ lower;
}

/// XXH3 avalanche finalizer for 64-bit values.
inline uint64_t avalanche64(uint64_t h)
{
    h ^= h >> 37;
    h *= UINT64_C(0x165667919E3779F9);
    h ^= h >> 32;
    return h;
}

/// XXH3 rrmxmx finalizer (used for medium-length inputs).
inline uint64_t rrmxmx(uint64_t h, uint64_t length)
{
    h ^= ((h << 49) | (h >> 15)) ^ ((h << 24) | (h >> 40));
    h *= UINT64_C(0x9FB21C651E98DF25);
    h ^= (h >> 35) + length;
    h *= UINT64_C(0x9FB21C651E98DF25);
    h ^= h >> 28;
    return h;
}

/// Hashes input of length 1-3 bytes.
inline uint64_t hashLen1to3(const uint8_t* input, uint64_t length, const uint8_t* secret, uint64_t seed)
{
    const auto c1 = input[0];
    const auto c2 = input[length >> 1];
    const auto c3 = input[length - 1];
    const auto combined = (static_cast<uint32_t>(c1) << 16) | (static_cast<uint32_t>(c2) << 24) | (static_cast<uint32_t>(c3) << 0)
        | (static_cast<uint32_t>(length) << 8);
    const auto bitflip = (static_cast<uint64_t>(readLE32(secret)) ^ static_cast<uint64_t>(readLE32(secret + 4))) + seed;
    return static_cast<uint64_t>(combined) ^ bitflip;
}

/// Hashes input of length 4-8 bytes.
inline uint64_t hashLen4to8(const uint8_t* input, uint64_t length, const uint8_t* secret, uint64_t seed)
{
    seed ^= static_cast<uint64_t>(static_cast<uint32_t>(seed) & 0xFFFFFFFF) << 32;
    const auto input1 = readLE32(input);
    const auto input2 = readLE32(input + length - 4);
    const auto bitflip = (readLE64(secret + 8) ^ readLE64(secret + 16)) - seed;
    const auto combined = static_cast<uint64_t>(input2) | (static_cast<uint64_t>(input1) << 32);
    return rrmxmx(combined ^ bitflip, length);
}

/// Hashes input of length 9-16 bytes.
inline uint64_t hashLen9to16(const uint8_t* input, uint64_t length, const uint8_t* secret, uint64_t seed)
{
    const auto bitflip1 = (readLE64(secret + 24) ^ readLE64(secret + 32)) + seed;
    const auto bitflip2 = (readLE64(secret + 40) ^ readLE64(secret + 48)) - seed;
    const auto inputLo = readLE64(input) ^ bitflip1;
    const auto inputHi = readLE64(input + length - 8) ^ bitflip2;
    const auto acc = length + (inputLo + inputHi) + mul128Fold64(inputLo, inputHi);
    return avalanche64(acc);
}

/// Hashes input of length 0 bytes.
inline uint64_t hashLen0(const uint8_t* /*input*/, uint64_t /*length*/, const uint8_t* secret, uint64_t seed)
{
    const auto bitflip = readLE64(secret + 56) ^ readLE64(secret + 64);
    return avalanche64(seed ^ bitflip);
}

/// Hashes input of length 17-128 bytes.
inline uint64_t hashLen17to128(const uint8_t* input, uint64_t length, const uint8_t* secret, uint64_t seed)
{
    auto acc = length * PRIME64_1;
    if (length > 32)
    {
        if (length > 64)
        {
            if (length > 96)
            {
                acc += mul128Fold64(readLE64(input + 48) ^ readLE64(secret + 96), readLE64(input + 56) ^ readLE64(secret + 104));
                acc += mul128Fold64(
                    readLE64(input + length - 64) ^ readLE64(secret + 112), readLE64(input + length - 56) ^ readLE64(secret + 120));
            }
            acc += mul128Fold64(readLE64(input + 32) ^ readLE64(secret + 64), readLE64(input + 40) ^ readLE64(secret + 72));
            acc += mul128Fold64(
                readLE64(input + length - 48) ^ readLE64(secret + 80), readLE64(input + length - 40) ^ readLE64(secret + 88));
        }
        acc += mul128Fold64(readLE64(input + 16) ^ readLE64(secret + 32), readLE64(input + 24) ^ readLE64(secret + 40));
        acc += mul128Fold64(readLE64(input + length - 32) ^ readLE64(secret + 48), readLE64(input + length - 24) ^ readLE64(secret + 56));
    }
    acc += mul128Fold64(readLE64(input) ^ readLE64(secret + seed), readLE64(input + 8) ^ readLE64(secret + 8));
    acc += mul128Fold64(readLE64(input + length - 16) ^ readLE64(secret + 16), readLE64(input + length - 8) ^ readLE64(secret + 24));
    return avalanche64(acc);
}

/// Hashes input of length 129-240 bytes.
inline uint64_t hashLen129to240(const uint8_t* input, uint64_t length, const uint8_t* secret, [[maybe_unused]] uint64_t seed)
{
    constexpr uint64_t startOffset = 3;
    constexpr uint64_t lastRoundSecretOffset = 136 - 17; /// = 119

    auto acc = length * PRIME64_1;
    const auto nbRounds = length / 16;

    for (uint64_t i = 0; i < 8; ++i)
    {
        acc += mul128Fold64(
            readLE64(input + 16 * i) ^ readLE64(secret + 16 * i), readLE64(input + 16 * i + 8) ^ readLE64(secret + 16 * i + 8));
    }
    acc = avalanche64(acc);

    for (uint64_t i = 8; i < nbRounds; ++i)
    {
        acc += mul128Fold64(
            readLE64(input + 16 * i) ^ readLE64(secret + 16 * (i - 8) + startOffset),
            readLE64(input + 16 * i + 8) ^ readLE64(secret + 16 * (i - 8) + startOffset + 8));
    }
    acc += mul128Fold64(
        readLE64(input + length - 16) ^ readLE64(secret + lastRoundSecretOffset),
        readLE64(input + length - 8) ^ readLE64(secret + lastRoundSecretOffset + 8));
    return avalanche64(acc);
}

} /// namespace XXH3Constants

/// Computes XXH3-64 hash over a raw byte buffer.
uint64_t xxh3HashBytes(void* data, uint64_t length)
{
    const auto* input = static_cast<const uint8_t*>(data);
    const auto* secret = XXH3Constants::DEFAULT_SECRET;
    constexpr uint64_t seed = 0;

    if (length <= 16)
    {
        if (length > 8)
        {
            return XXH3Constants::hashLen9to16(input, length, secret, seed);
        }
        if (length >= 4)
        {
            return XXH3Constants::hashLen4to8(input, length, secret, seed);
        }
        if (length > 0)
        {
            return XXH3Constants::hashLen1to3(input, length, secret, seed);
        }
        return XXH3Constants::hashLen0(input, length, secret, seed);
    }
    if (length <= 128)
    {
        return XXH3Constants::hashLen17to128(input, length, secret, seed);
    }
    if (length <= 240)
    {
        return XXH3Constants::hashLen129to240(input, length, secret, seed);
    }

    /// For inputs > 240 bytes, use a simplified long-input path.
    /// Full XXH3 uses a stripe-based accumulator with 8 lanes; here we use an iterative approach
    /// processing 32-byte chunks with secret rotation for good hash quality.
    auto acc = length * XXH3Constants::PRIME64_1;
    const auto nbChunks = (length - 1) / 32;
    for (uint64_t i = 0; i < nbChunks; ++i)
    {
        const auto secretOffset = (i % 6) * 16;
        acc += XXH3Constants::mul128Fold64(
            XXH3Constants::readLE64(input + 32 * i) ^ XXH3Constants::readLE64(secret + secretOffset),
            XXH3Constants::readLE64(input + 32 * i + 8) ^ XXH3Constants::readLE64(secret + secretOffset + 8));
        acc += XXH3Constants::mul128Fold64(
            XXH3Constants::readLE64(input + 32 * i + 16) ^ XXH3Constants::readLE64(secret + secretOffset + 16),
            XXH3Constants::readLE64(input + 32 * i + 24) ^ XXH3Constants::readLE64(secret + secretOffset + 24));
    }
    /// Process remaining bytes.
    acc += XXH3Constants::mul128Fold64(
        XXH3Constants::readLE64(input + length - 16) ^ XXH3Constants::readLE64(secret + 119),
        XXH3Constants::readLE64(input + length - 8) ^ XXH3Constants::readLE64(secret + 127));
    return XXH3Constants::avalanche64(acc);
}

/// Computes XXH3-64 for a single VarVal by treating its 8 bytes as input.
/// Uses the 9-16 byte path of XXH3 for a single 64-bit value (8 bytes).
VarVal xxh3VarVal(const VarVal& input)
{
    auto value = input.getRawValueAs<nautilus::val<uint64_t>>();

    /// XXH3 for 8-byte input uses the len4to8 path.
    /// We inline the mixing steps using nautilus val operations.
    constexpr uint64_t seed = 0;
    constexpr auto secretWord1 = XXH3Constants::readLE64(XXH3Constants::DEFAULT_SECRET + 8);
    constexpr auto secretWord2 = XXH3Constants::readLE64(XXH3Constants::DEFAULT_SECRET + 16);
    constexpr auto bitflip = secretWord1 ^ secretWord2;
    constexpr uint64_t inputLen = 8;

    /// For 4-8 bytes: split into two 32-bit reads and combine.
    /// Since we have exactly 8 bytes, both reads cover the full value.
    auto inputLo = value & nautilus::val<uint64_t>(0xFFFFFFFF);
    auto inputHi = value >> nautilus::val<uint64_t>(32);

    /// Combine: input2 | (input1 << 32) — for 8-byte input this reconstructs the original value.
    auto combined = inputLo | (inputHi << nautilus::val<uint64_t>(32));
    combined = combined ^ nautilus::val<uint64_t>(bitflip - seed);

    /// rrmxmx finalizer
    combined = combined ^ ((combined << nautilus::val<uint64_t>(49)) | (combined >> nautilus::val<uint64_t>(15)))
        ^ ((combined << nautilus::val<uint64_t>(24)) | (combined >> nautilus::val<uint64_t>(40)));
    combined = combined * nautilus::val<uint64_t>(UINT64_C(0x9FB21C651E98DF25));
    combined = combined ^ ((combined >> nautilus::val<uint64_t>(35)) + nautilus::val<uint64_t>(inputLen));
    combined = combined * nautilus::val<uint64_t>(UINT64_C(0x9FB21C651E98DF25));
    combined = combined ^ (combined >> nautilus::val<uint64_t>(28));

    return VarVal(combined);
}

HashFunction::HashValue XXH3HashFunction::init() const
{
    return nautilus::val<uint64_t>(0);
}

std::unique_ptr<HashFunction> XXH3HashFunction::clone() const
{
    return std::make_unique<XXH3HashFunction>(*this);
}

HashFunction::HashValue XXH3HashFunction::calculate(HashValue& hash, const VarVal& value) const
{
    return value
        .customVisit(
            [&]<typename T>(const T& val) -> VarVal
            {
                if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    const auto& varSizedContent = val;
                    return hash ^ nautilus::invoke(xxh3HashBytes, varSizedContent.getContent(), varSizedContent.getSize());
                }
                else
                {
                    return VarVal{hash} ^ xxh3VarVal(static_cast<nautilus::val<uint64_t>>(val));
                }
            })
        .getRawValueAs<HashValue>();
}

HashFunctionRegistryReturnType HashFunctionGeneratedRegistrar::RegisterXXH3HashFunction(HashFunctionRegistryArguments)
{
    return std::make_unique<XXH3HashFunction>();
}
}
