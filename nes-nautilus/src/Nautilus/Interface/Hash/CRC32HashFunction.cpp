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
#include <Nautilus/Interface/Hash/CRC32HashFunction.hpp>

#include <array>
#include <cstdint>
#include <memory>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <nautilus/function.hpp>
#include <HashFunctionRegistry.hpp>

namespace NES
{
namespace
{

/// Number of entries in the CRC32 lookup table (one per byte value).
constexpr uint32_t CRC32_TABLE_SIZE = 256;
/// Number of bits in a byte.
constexpr uint32_t BITS_PER_BYTE = 8;
/// CRC32 initial/final XOR mask.
constexpr uint32_t CRC32_XOR_MASK = 0xFFFFFFFF;
/// Byte mask for extracting the lowest 8 bits.
constexpr uint64_t BYTE_MASK = 0xFF;
/// Number of bytes in a 64-bit value.
constexpr int BYTES_IN_UINT64 = 8;

/// Generates the CRC32 lookup table at compile time using the standard polynomial 0xEDB88320.
consteval std::array<uint32_t, CRC32_TABLE_SIZE> generateCRC32Table()
{
    constexpr uint32_t polynomial = 0xEDB88320;
    std::array<uint32_t, CRC32_TABLE_SIZE> table{};
    for (uint32_t i = 0; i < CRC32_TABLE_SIZE; ++i)
    {
        uint32_t crc = i;
        for (uint32_t j = 0; j < BITS_PER_BYTE; ++j)
        {
            if ((crc & 1) != 0)
            {
                crc = (crc >> 1) ^ polynomial;
            }
            else
            {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }
    return table;
}

static constexpr auto crc32Table = generateCRC32Table();

/// Computes CRC32 over a raw byte buffer using the precomputed lookup table.
uint64_t crc32Bytes(void* data, uint64_t length)
{
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = CRC32_XOR_MASK;
    for (uint64_t i = 0; i < length; ++i)
    {
        crc = (crc >> BITS_PER_BYTE) ^ crc32Table[(crc ^ bytes[i]) & BYTE_MASK];
    }
    return static_cast<uint64_t>(crc ^ CRC32_XOR_MASK);
}

/// Computes CRC32 for a single VarVal by treating its bytes as input.
VarVal crc32VarVal(const VarVal& input)
{
    auto value = input.getRawValueAs<nautilus::val<uint64_t>>();

    /// Process each byte of the 64-bit value through the CRC32 table.
    /// We unfold this to avoid a loop over nautilus values.
    auto crc = nautilus::val<uint64_t>(CRC32_XOR_MASK);
    for (int i = 0; i < BYTES_IN_UINT64; ++i)
    {
        auto byte = (value >> nautilus::val<uint64_t>(static_cast<uint64_t>(i) * BITS_PER_BYTE)) & nautilus::val<uint64_t>(BYTE_MASK);
        auto tableIndex = (crc ^ byte) & nautilus::val<uint64_t>(BYTE_MASK);
        /// We invoke a helper to perform the table lookup at runtime.
        crc = (crc >> nautilus::val<uint64_t>(BITS_PER_BYTE))
            ^ nautilus::invoke(+[](uint64_t idx) -> uint64_t { return crc32Table[idx]; }, tableIndex);
    }
    crc = crc ^ nautilus::val<uint64_t>(CRC32_XOR_MASK);
    return {crc};
}

} /// anonymous namespace

HashFunction::HashValue CRC32HashFunction::init() const
{
    return {nautilus::val<uint64_t>(0)};
}

std::unique_ptr<HashFunction> CRC32HashFunction::clone() const
{
    return std::make_unique<CRC32HashFunction>(*this);
}

HashFunction::HashValue CRC32HashFunction::calculate(HashValue& hash, const VarVal& value) const
{
    return value
        .customVisit(
            [&]<typename T>(const T& val) -> VarVal
            {
                if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    const auto& varSizedContent = val;
                    return hash ^ nautilus::invoke(crc32Bytes, varSizedContent.getContent(), varSizedContent.getSize());
                }
                else
                {
                    return VarVal{hash} ^ crc32VarVal(static_cast<nautilus::val<uint64_t>>(val));
                }
            })
        .getRawValueAs<HashValue>();
}

HashFunctionRegistryReturnType HashFunctionGeneratedRegistrar::RegisterCRC32HashFunction(HashFunctionRegistryArguments)
{
    return std::make_unique<CRC32HashFunction>();
}
}
