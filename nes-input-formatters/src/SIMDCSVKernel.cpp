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

#include <SIMDCSVKernel.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
    #include <immintrin.h>
#elif defined(__aarch64__)
    #include <arm_neon.h>
#endif

/// This translation unit holds the only architecture-specific code in the SIMDCSV indexer.
/// Each kernel produces, per 64-byte block, the comma/newline/quote bitmasks; all higher-level logic
/// (quote masking, field-separator combine, bit walk, offset emission) is portable and lives in
/// SIMDCSVScan.hpp. The AVX2 kernel is compiled with a per-function `target("avx2")` attribute so it
/// emits AVX2 code while the rest of the TU stays at the generic build baseline; it is reached only
/// through a function pointer that is installed when `__builtin_cpu_supports("avx2")` is true.
namespace NES::SimdCsv
{
namespace
{

/// Scalar fallback: always compiled (used as the `#else` kernel and as the reference in the
/// differential test). Builds the masks one byte at a time.
void computeBlocksScalar(const char* data, const size_t numBlocks, BlockBits* out, const char fieldDelim, const char tupleDelim)
{
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const char* block = data + (blk * 64);
        uint64_t comma = 0;
        uint64_t newline = 0;
        uint64_t quote = 0;
        for (unsigned i = 0; i < 64; ++i)
        {
            const char byte = block[i];
            const uint64_t bit = uint64_t{1} << i;
            comma |= (byte == fieldDelim) ? bit : 0;
            newline |= (byte == tupleDelim) ? bit : 0;
            quote |= (byte == '"') ? bit : 0;
        }
        out[blk] = BlockBits{comma, newline, quote};
    }
}

#if defined(__x86_64__) || defined(_M_X64)

/// SSE2 is part of the x86-64 baseline, so this kernel is always runnable on x86-64. 4x16-byte loads.
void computeBlocksSse2(const char* data, const size_t numBlocks, BlockBits* out, const char fieldDelim, const char tupleDelim)
{
    const __m128i vComma = _mm_set1_epi8(fieldDelim);
    const __m128i vNewline = _mm_set1_epi8(tupleDelim);
    const __m128i vQuote = _mm_set1_epi8('"');
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const char* block = data + (blk * 64);
        uint64_t comma = 0;
        uint64_t newline = 0;
        uint64_t quote = 0;
        for (unsigned j = 0; j < 4; ++j)
        {
            const __m128i in = _mm_loadu_si128(reinterpret_cast<const __m128i*>(block + (j * 16)));
            const auto cm = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(in, vComma)));
            const auto nm = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(in, vNewline)));
            const auto qm = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(in, vQuote)));
            comma |= static_cast<uint64_t>(cm) << (j * 16);
            newline |= static_cast<uint64_t>(nm) << (j * 16);
            quote |= static_cast<uint64_t>(qm) << (j * 16);
        }
        out[blk] = BlockBits{comma, newline, quote};
    }
}

/// AVX2 kernel: 2x32-byte loads. Compiled with the avx2 target attribute so it codegens AVX2 inside an
/// otherwise generic build; only ever invoked through the dispatch pointer when the CPU supports AVX2.
__attribute__((target("avx2"))) void
computeBlocksAvx2(const char* data, const size_t numBlocks, BlockBits* out, const char fieldDelim, const char tupleDelim)
{
    const __m256i vComma = _mm256_set1_epi8(fieldDelim);
    const __m256i vNewline = _mm256_set1_epi8(tupleDelim);
    const __m256i vQuote = _mm256_set1_epi8('"');
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const char* block = data + (blk * 64);
        uint64_t comma = 0;
        uint64_t newline = 0;
        uint64_t quote = 0;
        for (unsigned j = 0; j < 2; ++j)
        {
            const __m256i in = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block + (j * 32)));
            const auto cm = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, vComma)));
            const auto nm = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, vNewline)));
            const auto qm = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, vQuote)));
            comma |= static_cast<uint64_t>(cm) << (j * 32);
            newline |= static_cast<uint64_t>(nm) << (j * 32);
            quote |= static_cast<uint64_t>(qm) << (j * 32);
        }
        out[blk] = BlockBits{comma, newline, quote};
    }
}

#elif defined(__aarch64__)

/// NEON has no movemask instruction; emulate it by AND-ing the 0x00/0xFF compare result with a
/// per-lane bit-position constant and horizontally adding each 8-lane half (vaddv_u8, armv8-a).
inline uint64_t neonMoveMask(const uint8x16_t cmp, const uint8x16_t bitmask)
{
    const uint8x16_t anded = vandq_u8(cmp, bitmask);
    const auto low = static_cast<uint64_t>(vaddv_u8(vget_low_u8(anded)));
    const auto high = static_cast<uint64_t>(vaddv_u8(vget_high_u8(anded)));
    return low | (high << 8);
}

/// NEON kernel: 4x16-byte loads, armv8-a baseline (always runnable on arm64).
void computeBlocksNeon(const char* data, const size_t numBlocks, BlockBits* out, const char fieldDelim, const char tupleDelim)
{
    static const uint8_t bitmaskBytes[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t bitmask = vld1q_u8(bitmaskBytes);
    const uint8x16_t vComma = vdupq_n_u8(static_cast<uint8_t>(fieldDelim));
    const uint8x16_t vNewline = vdupq_n_u8(static_cast<uint8_t>(tupleDelim));
    const uint8x16_t vQuote = vdupq_n_u8(static_cast<uint8_t>('"'));
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const auto* block = reinterpret_cast<const uint8_t*>(data + (blk * 64));
        uint64_t comma = 0;
        uint64_t newline = 0;
        uint64_t quote = 0;
        for (unsigned j = 0; j < 4; ++j)
        {
            const uint8x16_t in = vld1q_u8(block + (j * 16));
            comma |= neonMoveMask(vceqq_u8(in, vComma), bitmask) << (j * 16);
            newline |= neonMoveMask(vceqq_u8(in, vNewline), bitmask) << (j * 16);
            quote |= neonMoveMask(vceqq_u8(in, vQuote), bitmask) << (j * 16);
        }
        out[blk] = BlockBits{comma, newline, quote};
    }
}

#endif

}

ComputeBlocksFn selectComputeBlocks()
{
#if defined(__x86_64__) || defined(_M_X64)
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx2"))
    {
        return &computeBlocksAvx2;
    }
    return &computeBlocksSse2;
#elif defined(__aarch64__)
    return &computeBlocksNeon;
#else
    return &computeBlocksScalar;
#endif
}

std::vector<KernelEntry> availableKernels()
{
    std::vector<KernelEntry> kernels;
    kernels.push_back({"scalar", &computeBlocksScalar});
#if defined(__x86_64__) || defined(_M_X64)
    kernels.push_back({"sse2", &computeBlocksSse2});
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx2"))
    {
        kernels.push_back({"avx2", &computeBlocksAvx2});
    }
#elif defined(__aarch64__)
    kernels.push_back({"neon", &computeBlocksNeon});
#endif
    return kernels;
}

}
