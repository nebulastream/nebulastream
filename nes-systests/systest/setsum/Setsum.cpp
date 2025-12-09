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

#include "Setsum.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <ostream>
#include <string_view>

namespace
{
// MurmurHash3 constants - carefully chosen magic numbers by Austin Appleby
// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
// These values are optimized through mathematical analysis to minimize collisions
// and ensure uniform hash distribution across the output space
constexpr uint32_t MURMUR_C1 = 0xcc9e2d51;           // Primary mixing constant
constexpr uint32_t MURMUR_C2 = 0x1b873593;           // Secondary mixing constant
constexpr uint32_t MURMUR_MULTIPLIER = 5;            // Main loop multiplier
constexpr uint32_t MURMUR_OFFSET = 0xe6546b64;       // Main loop offset
constexpr uint32_t COLUMN_DELTA = 0x85ebca6b;        // Per-column differentiation for 8 independent hashes

constexpr uint32_t FINALIZE_MIX1 = 0x85ebca6b;       // Finalization avalanche mixer
constexpr uint32_t FINALIZE_MIX2 = 0xc2b2ae35;       // Finalization avalanche mixer

constexpr size_t BLOCK_SIZE = 4;
constexpr size_t NUM_COLUMNS = 8;

// Bit shifts for hash finalization and mixing
constexpr uint32_t SHIFT_16 = 16;
constexpr uint32_t SHIFT_15 = 15;
constexpr uint32_t SHIFT_13 = 13;
constexpr uint32_t SHIFT_8 = 8;

/// 32-bit left rotation
constexpr uint32_t rotateLeft32(uint32_t value, uint32_t shift)
{
    constexpr uint32_t bits_in_uint32 = 32;
    return (value << shift) | (value >> (bits_in_uint32 - shift));
}

/// Mix a block value according to MurmurHash3 algorithm
constexpr uint32_t mixBlock(uint32_t block)
{
    block *= MURMUR_C1;
    block = rotateLeft32(block, SHIFT_15);
    block *= MURMUR_C2;
    return block;
}

/// Apply finalization mixing to ensure avalanche effect
constexpr uint32_t finalizeMix(uint32_t hash, uint32_t length)
{
    hash ^= length;
    hash ^= hash >> SHIFT_16;
    hash *= FINALIZE_MIX1;
    hash ^= hash >> SHIFT_13;
    hash *= FINALIZE_MIX2;
    hash ^= hash >> SHIFT_16;
    return hash;
}

/**
 * @brief MurmurHash3-based hash producing 256 bits (8 × 32-bit columns)
 * 
 * This custom variant generates 8 independent hash values by using different
 * initial seeds and mixing each column with a unique delta value.
 * 
 * @param key Pointer to data to hash
 * @param len Length of data in bytes
 * @return Array of 8 hash values (256 bits total)
 */
std::array<uint32_t, NUM_COLUMNS> murmurHash3_256(const void* key, const size_t len)
{
    const auto* data = static_cast<const uint8_t*>(key);
    const size_t numBlocks = len / BLOCK_SIZE;

    // Seeds for hashing
    std::array<uint32_t, NUM_COLUMNS> columns = {
        0x9747b28c, 0x7feb352d, 0x4f2e6a1b, 0x3c9d5e8f,
        0x2b8a4c7e, 0x1a6b3d9c, 0x0f5e2a8b, 0x8d7c6b5a
    };

    // Process input in 4-byte blocks
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - required for binary data hashing
    const auto* blocks = reinterpret_cast<const uint32_t*>(data);
    for (size_t blockIdx = 0; blockIdx < numBlocks; blockIdx++)
    {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index) - loop bounds ensure safe access
        const uint32_t mixedBlock = mixBlock(blocks[blockIdx]);

        // Update each column with unique mixing
        for (size_t col = 0; col < NUM_COLUMNS; col++)
        {
            const uint32_t columnOffset = static_cast<uint32_t>(col) * COLUMN_DELTA;
            columns.at(col) ^= mixedBlock + columnOffset;
            columns.at(col) = rotateLeft32(columns.at(col), SHIFT_13);
            columns.at(col) = columns.at(col) * MURMUR_MULTIPLIER + MURMUR_OFFSET;
        }
    }

    // Process remaining bytes (0-3 bytes)
    const size_t tailOffset = numBlocks * BLOCK_SIZE;
    const size_t remainingBytes = len & 3;

    if (remainingBytes > 0)
    {
        uint32_t tailBlock = 0;

        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - required for byte-level access in hash
        if (remainingBytes >= 3) { tailBlock ^= static_cast<uint32_t>(data[tailOffset + 2]) << SHIFT_16; }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - required for byte-level access in hash
        if (remainingBytes >= 2) { tailBlock ^= static_cast<uint32_t>(data[tailOffset + 1]) << SHIFT_8; }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - required for byte-level access in hash
        if (remainingBytes >= 1) { tailBlock ^= static_cast<uint32_t>(data[tailOffset + 0]); }

        const uint32_t mixedTail = mixBlock(tailBlock);
        for (auto& column : columns)
        {
            column ^= mixedTail;
        }
    }

    // Apply finalization mixing to each column
    for (auto& column : columns)
    {
        column = finalizeMix(column, static_cast<uint32_t>(len));
    }

    return columns;
}
}

void Setsum::add(std::string_view data)
{
    const auto hash = murmurHash3_256(data.data(), data.size());

    // Add each hash value to its corresponding column with modulo prime
    for (size_t i = 0; i < NUM_COLUMNS; i++)
    {
        const uint32_t value = hash.at(i) % PRIMES.at(i);
        
        // Atomic compare-exchange loop for modular addition
        uint32_t oldValue = columns.at(i).load(std::memory_order::relaxed);
        uint32_t newValue = 0;
        
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) - compare_exchange_weak requires do-while pattern
        do
        {
            // Use uint64_t to prevent overflow when adding
            newValue = static_cast<uint32_t>((static_cast<uint64_t>(oldValue) + value) % PRIMES.at(i));
        }
        while (!columns.at(i).compare_exchange_weak(oldValue, newValue, 
                                                   std::memory_order::relaxed));
    }
}

void Setsum::remove(std::string_view data)
{
    // Hash the input data to get 8 x uint32_t values
    const auto hash = murmurHash3_256(data.data(), data.size());

    // Subtract by adding the modular inverse: inverse(x) = prime - x
    for (size_t i = 0; i < NUM_COLUMNS; i++)
    {
        const uint32_t value = hash.at(i) % PRIMES.at(i);
        const uint32_t inverse = (PRIMES.at(i) - value) % PRIMES.at(i);
        
        // Atomic compare-exchange loop for modular subtraction
        uint32_t oldValue = columns.at(i).load(std::memory_order::relaxed);
        uint32_t newValue = 0;
        
        // NOLINTNEXTLINE(cppcoreguidelines-avoid-do-while) - compare_exchange_weak requires do-while pattern
        do
        {
            // Use uint64_t to prevent overflow when adding inverse
            newValue = static_cast<uint32_t>((static_cast<uint64_t>(oldValue) + inverse) % PRIMES.at(i));
        }
        while (!columns.at(i).compare_exchange_weak(oldValue, newValue, 
                                                   std::memory_order::relaxed));
    }
}

bool Setsum::operator==(const Setsum& other) const
{
    for (size_t i = 0; i < NUM_COLUMNS; i++)
    {
        if (columns.at(i).load(std::memory_order::relaxed) != 
            other.columns.at(i).load(std::memory_order::relaxed))
        {
            return false;
        }
    }
    return true;
}

std::ostream& operator<<(std::ostream& os, const Setsum& obj)
{
    os << "setsum: [";
    for (size_t i = 0; i < NUM_COLUMNS; i++)
    {
        os << obj.columns.at(i).load();
        constexpr size_t lastColumnIdx = NUM_COLUMNS - 1;
        if (i < lastColumnIdx)
        {
            os << ", ";
        }
    }
    os << "]";
    return os;
}
