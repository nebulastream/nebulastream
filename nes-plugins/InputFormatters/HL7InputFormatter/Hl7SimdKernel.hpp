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
#include <cstddef>
#include <cstdint>
#include <vector>

/// Portable SIMD building block for the SIMDHL7 indexer, following the SIMDCSV kernel's design
/// (see nes-input-formatters/private/SIMDCSVKernel.hpp; those headers are private to the core
/// module, so the HL7 plugin carries its own copy of the pattern).
///
/// The only architecture-specific operation is "load 64 bytes, compare-equal to a broadcast byte,
/// produce a 64-bit bitmask". HL7 needs six compares per block: the four structural delimiters
/// (segment <CR>, field |, component ^, subcomponent &) OR-combined into ONE structural mask, plus
/// the two bytes of the MLLP message trailer "\x1C\r" as separate masks (the driver pairs them,
/// including across block boundaries). Runtime CPU dispatch: SSE2 baseline / AVX2 on x86-64, NEON
/// on arm64, scalar everywhere else.
namespace NES::SimdHl7
{

/// Per-64-byte-block bitmasks. Bit `i` corresponds to byte `i` of the block (LSB == first byte).
struct BlockBits
{
    uint64_t structural; ///< bytes in the structural delimiter class (segment/field/component/subcomponent)
    uint64_t msgFirst; ///< bytes equal to the message delimiter's FIRST byte (<FS>)
    uint64_t msgSecond; ///< bytes equal to the message delimiter's SECOND byte (<CR>)
};

/// Fills `out[0..numBlocks)` with the bitmasks of the `numBlocks * 64` bytes starting at `data`.
/// Reads exactly `numBlocks * 64` bytes. `structuralChars` holds the four structural delimiters
/// (duplicate an enabled one into a disabled slot -- ORing a duplicate compare is harmless).
using ComputeBlocksFn = void (*)(
    const char* data, size_t numBlocks, BlockBits* out, const std::array<char, 4>& structuralChars, char msgFirst, char msgSecond);

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
/// scalar fallback). AVX2 is only listed when the CPU actually supports it.
std::vector<KernelEntry> availableKernels();

}
