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

#include <Hl7SimdKernel.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
    #include <immintrin.h>
#elif defined(__aarch64__)
    #include <arm_neon.h>
#endif

/// The only architecture-specific code of the SIMDHL7 indexer (see Hl7SimdKernel.hpp). Follows the
/// SIMDCSV kernel TU byte-for-byte in structure: per-64-byte-block broadcast-compare bitmasks with
/// runtime CPU dispatch; the AVX2 kernel carries a per-function target attribute and is only reached
/// through the dispatch pointer when the CPU supports it.
namespace NES::SimdHl7
{
namespace
{

/// Scalar fallback: always compiled (also the reference kernel in the differential test).
void computeBlocksScalar(
    const char* data,
    const size_t numBlocks,
    BlockBits* out,
    const std::array<char, 4>& structuralChars,
    const char msgFirst,
    const char msgSecond)
{
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const char* block = data + (blk * 64);
        uint64_t structural = 0;
        uint64_t first = 0;
        uint64_t second = 0;
        for (unsigned i = 0; i < 64; ++i)
        {
            const char byte = block[i];
            const uint64_t bit = uint64_t{1} << i;
            structural
                |= (byte == structuralChars[0] || byte == structuralChars[1] || byte == structuralChars[2] || byte == structuralChars[3])
                ? bit
                : 0;
            first |= (byte == msgFirst) ? bit : 0;
            second |= (byte == msgSecond) ? bit : 0;
        }
        out[blk] = BlockBits{structural, first, second};
    }
}

#if defined(__x86_64__) || defined(_M_X64)

/// SSE2 baseline kernel: 4x16-byte loads, six compares each.
void computeBlocksSse2(
    const char* data,
    const size_t numBlocks,
    BlockBits* out,
    const std::array<char, 4>& structuralChars,
    const char msgFirst,
    const char msgSecond)
{
    const __m128i vS0 = _mm_set1_epi8(structuralChars[0]);
    const __m128i vS1 = _mm_set1_epi8(structuralChars[1]);
    const __m128i vS2 = _mm_set1_epi8(structuralChars[2]);
    const __m128i vS3 = _mm_set1_epi8(structuralChars[3]);
    const __m128i vFirst = _mm_set1_epi8(msgFirst);
    const __m128i vSecond = _mm_set1_epi8(msgSecond);
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const char* block = data + (blk * 64);
        uint64_t structural = 0;
        uint64_t first = 0;
        uint64_t second = 0;
        for (unsigned j = 0; j < 4; ++j)
        {
            const __m128i in = _mm_loadu_si128(reinterpret_cast<const __m128i*>(block + (j * 16)));
            const __m128i structuralCmp = _mm_or_si128(
                _mm_or_si128(_mm_cmpeq_epi8(in, vS0), _mm_cmpeq_epi8(in, vS1)),
                _mm_or_si128(_mm_cmpeq_epi8(in, vS2), _mm_cmpeq_epi8(in, vS3)));
            const auto sm = static_cast<uint32_t>(_mm_movemask_epi8(structuralCmp));
            const auto fm = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(in, vFirst)));
            const auto cm = static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(in, vSecond)));
            structural |= static_cast<uint64_t>(sm) << (j * 16);
            first |= static_cast<uint64_t>(fm) << (j * 16);
            second |= static_cast<uint64_t>(cm) << (j * 16);
        }
        out[blk] = BlockBits{structural, first, second};
    }
}

/// AVX2 kernel: 2x32-byte loads; per-function target attribute, dispatch-guarded.
__attribute__((target("avx2"))) void computeBlocksAvx2(
    const char* data,
    const size_t numBlocks,
    BlockBits* out,
    const std::array<char, 4>& structuralChars,
    const char msgFirst,
    const char msgSecond)
{
    const __m256i vS0 = _mm256_set1_epi8(structuralChars[0]);
    const __m256i vS1 = _mm256_set1_epi8(structuralChars[1]);
    const __m256i vS2 = _mm256_set1_epi8(structuralChars[2]);
    const __m256i vS3 = _mm256_set1_epi8(structuralChars[3]);
    const __m256i vFirst = _mm256_set1_epi8(msgFirst);
    const __m256i vSecond = _mm256_set1_epi8(msgSecond);
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const char* block = data + (blk * 64);
        uint64_t structural = 0;
        uint64_t first = 0;
        uint64_t second = 0;
        for (unsigned j = 0; j < 2; ++j)
        {
            const __m256i in = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block + (j * 32)));
            const __m256i structuralCmp = _mm256_or_si256(
                _mm256_or_si256(_mm256_cmpeq_epi8(in, vS0), _mm256_cmpeq_epi8(in, vS1)),
                _mm256_or_si256(_mm256_cmpeq_epi8(in, vS2), _mm256_cmpeq_epi8(in, vS3)));
            const auto sm = static_cast<uint32_t>(_mm256_movemask_epi8(structuralCmp));
            const auto fm = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, vFirst)));
            const auto cm = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, vSecond)));
            structural |= static_cast<uint64_t>(sm) << (j * 32);
            first |= static_cast<uint64_t>(fm) << (j * 32);
            second |= static_cast<uint64_t>(cm) << (j * 32);
        }
        out[blk] = BlockBits{structural, first, second};
    }
}

#elif defined(__aarch64__)

/// NEON movemask emulation (see SIMDCSVKernel.cpp's neonMoveMaskBulk for the rationale).
inline uint64_t
neonMoveMaskBulk(const uint8x16_t p0, const uint8x16_t p1, const uint8x16_t p2, const uint8x16_t p3, const uint8x16_t bitmask)
{
    const uint8x16_t t0 = vandq_u8(p0, bitmask);
    const uint8x16_t t1 = vandq_u8(p1, bitmask);
    const uint8x16_t t2 = vandq_u8(p2, bitmask);
    const uint8x16_t t3 = vandq_u8(p3, bitmask);
    uint8x16_t sum0 = vpaddq_u8(t0, t1);
    const uint8x16_t sum1 = vpaddq_u8(t2, t3);
    sum0 = vpaddq_u8(sum0, sum1);
    sum0 = vpaddq_u8(sum0, sum0);
    return vgetq_lane_u64(vreinterpretq_u64_u8(sum0), 0);
}

/// NEON kernel: 4x16-byte loads, armv8-a baseline.
void computeBlocksNeon(
    const char* data,
    const size_t numBlocks,
    BlockBits* out,
    const std::array<char, 4>& structuralChars,
    const char msgFirst,
    const char msgSecond)
{
    static const uint8_t bitmaskBytes[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t bitmask = vld1q_u8(bitmaskBytes);
    const uint8x16_t vS0 = vdupq_n_u8(static_cast<uint8_t>(structuralChars[0]));
    const uint8x16_t vS1 = vdupq_n_u8(static_cast<uint8_t>(structuralChars[1]));
    const uint8x16_t vS2 = vdupq_n_u8(static_cast<uint8_t>(structuralChars[2]));
    const uint8x16_t vS3 = vdupq_n_u8(static_cast<uint8_t>(structuralChars[3]));
    const uint8x16_t vFirst = vdupq_n_u8(static_cast<uint8_t>(msgFirst));
    const uint8x16_t vSecond = vdupq_n_u8(static_cast<uint8_t>(msgSecond));
    const auto structuralCmp = [&](const uint8x16_t in)
    { return vorrq_u8(vorrq_u8(vceqq_u8(in, vS0), vceqq_u8(in, vS1)), vorrq_u8(vceqq_u8(in, vS2), vceqq_u8(in, vS3))); };
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const auto* block = reinterpret_cast<const uint8_t*>(data + (blk * 64));
        const uint8x16_t b0 = vld1q_u8(block);
        const uint8x16_t b1 = vld1q_u8(block + 16);
        const uint8x16_t b2 = vld1q_u8(block + 32);
        const uint8x16_t b3 = vld1q_u8(block + 48);
        const uint64_t structural = neonMoveMaskBulk(structuralCmp(b0), structuralCmp(b1), structuralCmp(b2), structuralCmp(b3), bitmask);
        const uint64_t first
            = neonMoveMaskBulk(vceqq_u8(b0, vFirst), vceqq_u8(b1, vFirst), vceqq_u8(b2, vFirst), vceqq_u8(b3, vFirst), bitmask);
        const uint64_t second
            = neonMoveMaskBulk(vceqq_u8(b0, vSecond), vceqq_u8(b1, vSecond), vceqq_u8(b2, vSecond), vceqq_u8(b3, vSecond), bitmask);
        out[blk] = BlockBits{structural, first, second};
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
