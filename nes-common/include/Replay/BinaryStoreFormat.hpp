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

#include <array>
#include <cstdint>
#include <optional>

namespace NES::Replay
{

enum class BinaryStoreCompressionCodec : uint32_t
{
    None = 0,
    Zstd = 1,
};

inline constexpr int32_t DEFAULT_BINARY_STORE_ZSTD_COMPRESSION_LEVEL = -3;

struct BinaryStoreSegmentHeader
{
    uint32_t decodedSize;
    uint32_t encodedSize;
};

inline constexpr std::array<char, 8> BINARY_STORE_MAGIC{'N', 'E', 'S', 'S', 'T', 'O', 'R', 'E'};
inline constexpr uint32_t BINARY_STORE_FORMAT_VERSION_RAW_ROWS = 1;
inline constexpr uint32_t BINARY_STORE_FORMAT_VERSION_SEGMENTED = 2;
inline constexpr uint8_t BINARY_STORE_ENDIANNESS_LITTLE = 1;

[[nodiscard]] inline constexpr uint32_t binaryStoreFlagsForCodec(const BinaryStoreCompressionCodec codec)
{
    return static_cast<uint32_t>(codec);
}

[[nodiscard]] inline constexpr std::optional<BinaryStoreCompressionCodec> binaryStoreCodecFromFlags(const uint32_t flags)
{
    switch (flags & 0xFFu)
    {
        case static_cast<uint32_t>(BinaryStoreCompressionCodec::None):
            return BinaryStoreCompressionCodec::None;
        case static_cast<uint32_t>(BinaryStoreCompressionCodec::Zstd):
            return BinaryStoreCompressionCodec::Zstd;
        default:
            return std::nullopt;
    }
}

[[nodiscard]] inline constexpr bool binaryStoreUsesSegmentedPayload(const uint32_t version)
{
    return version >= BINARY_STORE_FORMAT_VERSION_SEGMENTED;
}

[[nodiscard]] inline constexpr uint64_t binaryStoreHeaderSize(const uint32_t schemaLen)
{
    return static_cast<uint64_t>(
        BINARY_STORE_MAGIC.size() + sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t)
        + schemaLen);
}

}
