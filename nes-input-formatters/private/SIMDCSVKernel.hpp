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

#include <cstddef>
#include <cstdint>
#include <vector>

/// Portable SIMD building block for the SIMDCSV indexer, ported from the algorithm in
/// https://github.com/geofflangdale/simdcsv (Apache-2.0, by a simdjson author).
///
/// The only genuinely architecture-specific operation in the simdcsv algorithm is "load 64 bytes,
/// compare-equal to a broadcast byte, produce a 64-bit bitmask". Everything else (the quote
/// prefix-xor, the field-separator combine, the bit extraction and the offset walk) operates on
/// plain `uint64_t` and lives in the portable driver (see SIMDCSVScan.hpp).
///
/// Portability is achieved like simdjson: runtime CPU dispatch. On x86-64, SSE2 is the always
/// available baseline and AVX2 is selected at runtime; on arm64 NEON is the armv8-a baseline; a
/// scalar fallback covers every other target. The selected kernel is resolved once (per indexer
/// instance) via @ref selectComputeBlocks.
namespace NES::SimdCsv
{

/// Per-64-byte-block bitmasks. Bit `i` corresponds to byte `i` of the block (LSB == first byte).
struct BlockBits
{
    uint64_t comma; ///< bytes equal to the field delimiter
    uint64_t newline; ///< bytes equal to the tuple delimiter
    uint64_t quote; ///< bytes equal to '"'
};

/// Fills `out[0..numBlocks)` with the bitmasks of the `numBlocks * 64` bytes starting at `data`.
/// Reads exactly `numBlocks * 64` bytes; the caller guarantees that many bytes are valid.
using ComputeBlocksFn = void (*)(const char* data, size_t numBlocks, BlockBits* out, char fieldDelim, char tupleDelim);

/// Resolves the best block kernel for the current CPU (runtime dispatch). Cheap, but intended to be
/// called once and cached (the indexer stores the result as a member).
ComputeBlocksFn selectComputeBlocks();

/// A named block kernel. Used by the differential test to exercise every kernel that is both
/// compiled for and runnable on the current CPU against the same input.
struct KernelEntry
{
    const char* name;
    ComputeBlocksFn fn;
};

/// All block kernels that are compiled for and safe to run on the current CPU (always includes the
/// scalar fallback). AVX2 is only listed when the CPU actually supports it, so running every entry
/// can never raise SIGILL.
std::vector<KernelEntry> availableKernels();

/// Inclusive prefix-xor over the 64 bits (bit `i` becomes the XOR of bits `0..i`). Carry-less-multiply
/// free, fully portable: this is the in-block half of the simdcsv quote mask. The cross-block carry is
/// folded in by the driver.
inline uint64_t prefixXor(uint64_t bits)
{
    bits ^= bits << 1;
    bits ^= bits << 2;
    bits ^= bits << 4;
    bits ^= bits << 8;
    bits ^= bits << 16;
    bits ^= bits << 32;
    return bits;
}

}
