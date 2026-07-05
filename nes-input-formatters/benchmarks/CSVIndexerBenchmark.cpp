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

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include <SIMDCSVKernel.hpp>
#include <SIMDCSVScan.hpp>

#if defined(__x86_64__)
    #include <immintrin.h>
#endif

/// Self-contained micro-benchmark (no external benchmark library) for the CSV indexing phase:
/// "raw bytes -> field/tuple offsets", the work the InputFormatIndexer does before value parsing. It
/// measures the engine's REAL fast path -- the branchless flat band (`indexToBand` here ==
/// `SimdCsv::indexBandInto`, what SIMDCSVInputFormatIndexer::indexRawBuffer calls). The band vector is a
/// REAL sink: every structural offset is written to memory (capacity reused across iters), so the band's
/// memory traffic is measured exactly as the engine pays it.
///
/// TWO-AXIS scaling: number of columns N and per-field width W (chars/field) are swept INDEPENDENTLY.
///   row width  = N * (W + 1)             (does row width matter on its own?)
///   delim density = 1 / (W + 1)          (governs the per-delimiter flatten cost)
/// Holding W fixed and sweeping N varies row width at constant density; holding N fixed and sweeping W
/// varies density. Fit GB/s vs 1/W to split the cost: intercept = pure per-byte SIMD-scan ceiling,
/// slope = per-delimiter flatten+band-write cost.
///
/// OPTIMIZATION-HEADROOM diagnostics (x86): the production AVX2 kernel ALWAYS computes the quote mask
/// (3 compares+movemasks/block) and round-trips BlockBits through a scratch array via a function pointer.
/// For no-quote numeric data that quote compare is wasted, and the scratch round-trip is the deferred
/// "dispatch boundary" headroom. Four variants isolate this:
///   dispatched QA   -- exact engine path (quote-aware, fn-ptr kernel, BlockBits scratch)
///   dispatched noQA -- allow_commas_in_strings=false (skips only the driver prefixXor, NOT the quote SIMD compare)
///   fused QA        -- AVX2 inlined, no scratch/fn-ptr, quote-aware (isolates the dispatch/scratch tax)
///   fused noQuote   -- AVX2 inlined, quote compare SKIPPED entirely (the true no-quote scan ceiling)
///
/// Build (Release):  cmake --build {build} --target csv-indexer-benchmark
/// Run:  {build}/nes-input-formatters/benchmarks/csv-indexer-benchmark [N=1,4,16] [W=2,4,8,16,32,64] [iters=15] [MiB=64]
namespace
{

constexpr char fieldDelim = ',';
constexpr char tupleDelim = '\n';

/// ~`targetBytes` of N-column CSV, each field EXACTLY `fieldWidth` chars (digits, no quotes). The digit
/// values are irrelevant to indexing (it scans only for delimiters), so exact-width fields give precise
/// control of delimiter density = 1/(fieldWidth+1). Returns the row count via `outRows`.
std::string makeCsv(const uint64_t numFields, const unsigned fieldWidth, const size_t targetBytes, uint64_t& outRows)
{
    std::string csv;
    csv.reserve(targetBytes + 128);
    uint64_t counter = 0;
    std::string field(fieldWidth, '0');
    while (csv.size() < targetBytes)
    {
        /// Cheap varying digits so the field isn't a constant run (irrelevant to the scan, but avoids any
        /// pathological all-same-byte codegen). Fill W digits from the counter.
        uint64_t v = counter * 2654435761ULL;
        for (unsigned d = 0; d < fieldWidth; ++d)
        {
            field[d] = static_cast<char>('0' + (v % 10));
            v /= 10;
        }
        for (uint64_t f = 0; f < numFields; ++f)
        {
            if (f != 0)
            {
                csv += fieldDelim;
            }
            csv += field;
        }
        csv += tupleDelim;
        ++counter;
    }
    outRows = counter;
    return csv;
}

const char* simdKernelName()
{
    const char* name = "scalar";
    for (const auto& kernel : NES::SimdCsv::availableKernels())
    {
        if (std::string_view{kernel.name} != "scalar")
        {
            name = kernel.name;
        }
    }
    return name;
}

/// Branchless, unrolled bit-flatten (the simdcsv flatten_bits trick): write each set bit's position
/// (base + trailing-zeros) into `out`, advancing by popcount. Writes up to 8/16 slots unconditionally
/// for branch-free speed; the slots past the real count are overwritten by the next call, so `out`
/// must have >= 16 slots of slack beyond the true total. Returns the advanced pointer.
inline uint32_t* flattenBits(uint32_t* out, const uint32_t base, uint64_t bits)
{
    if (bits == 0)
    {
        return out;
    }
    const int cnt = __builtin_popcountll(bits);
    for (int k = 0; k < 8; ++k)
    {
        out[k] = base + static_cast<uint32_t>(__builtin_ctzll(bits));
        bits &= bits - 1;
    }
    if (cnt > 8)
    {
        for (int k = 8; k < 16; ++k)
        {
            out[k] = base + static_cast<uint32_t>(__builtin_ctzll(bits));
            bits &= bits - 1;
        }
    }
    if (cnt > 16)
    {
        for (int k = 16; k < cnt; ++k)
        {
            out[k] = base + static_cast<uint32_t>(__builtin_ctzll(bits));
            bits &= bits - 1;
        }
    }
    return out + cnt;
}

/// ENGINE-REAL fast path (== SimdCsv::indexBandInto): dispatched fn-ptr kernel -> BlockBits scratch ->
/// flatten. `quoteAware` toggles the driver's prefixXor quote handling (== allow_commas_in_strings).
size_t
indexToBandDispatched(uint32_t* out, const std::string_view view, const bool quoteAware, const NES::SimdCsv::ComputeBlocksFn computeBlocks)
{
    using NES::SimdCsv::BlockBits;
    const char* data = view.data();
    const size_t numBytes = view.size();
    uint32_t* cursor = out;
    uint64_t quoteCarry = 0;

    const size_t numBlocks = numBytes / 64;
    constexpr size_t chunkBlocks = 16;
    std::array<BlockBits, chunkBlocks> chunk{};
    for (size_t b = 0; b < numBlocks; b += chunkBlocks)
    {
        const size_t count = std::min(chunkBlocks, numBlocks - b);
        computeBlocks(data + (b * 64), count, chunk.data(), fieldDelim, tupleDelim);
        for (size_t i = 0; i < count; ++i)
        {
            const BlockBits bits = chunk[i];
            const auto blockBase = static_cast<uint32_t>((b + i) * 64);
            uint64_t fieldSep = 0;
            if (quoteAware)
            {
                uint64_t quoted = NES::SimdCsv::prefixXor(bits.quote);
                quoted ^= quoteCarry;
                quoteCarry = uint64_t{0} - (quoted >> 63);
                fieldSep = (bits.comma & ~quoted) | bits.newline;
            }
            else
            {
                fieldSep = bits.comma | bits.newline;
            }
            cursor = flattenBits(cursor, blockBase, fieldSep);
        }
    }
    for (size_t i = numBlocks * 64; i < numBytes; ++i)
    {
        const char byte = data[i];
        if (byte == tupleDelim || byte == fieldDelim)
        {
            *cursor++ = static_cast<uint32_t>(i);
        }
    }
    return static_cast<size_t>(cursor - out);
}

#if defined(__x86_64__)
/// DIAGNOSTIC (x86): the AVX2 kernel inlined per block -- no fn-ptr, no BlockBits scratch array, masks
/// stay in registers straight into flatten. `ComputeQuote=false` SKIPS the quote compare+movemask AND
/// the prefixXor entirely (the true no-quote ceiling); `true` computes quote + quote-aware field sep.
template <bool ComputeQuote>
size_t indexToBandFusedAvx2(uint32_t* out, const std::string_view view)
{
    const char* data = view.data();
    const size_t numBytes = view.size();
    uint32_t* cursor = out;
    uint64_t quoteCarry = 0;
    const __m256i vComma = _mm256_set1_epi8(fieldDelim);
    const __m256i vNewline = _mm256_set1_epi8(tupleDelim);
    const __m256i vQuote = _mm256_set1_epi8('"');

    const size_t numBlocks = numBytes / 64;
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
            comma |= static_cast<uint64_t>(cm) << (j * 32);
            newline |= static_cast<uint64_t>(nm) << (j * 32);
            if constexpr (ComputeQuote)
            {
                const auto qm = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(in, vQuote)));
                quote |= static_cast<uint64_t>(qm) << (j * 32);
            }
        }
        uint64_t fieldSep = 0;
        if constexpr (ComputeQuote)
        {
            uint64_t quoted = NES::SimdCsv::prefixXor(quote);
            quoted ^= quoteCarry;
            quoteCarry = uint64_t{0} - (quoted >> 63);
            fieldSep = (comma & ~quoted) | newline;
        }
        else
        {
            fieldSep = comma | newline;
        }
        cursor = flattenBits(cursor, static_cast<uint32_t>(blk * 64), fieldSep);
    }
    for (size_t i = numBlocks * 64; i < numBytes; ++i)
    {
        const char byte = data[i];
        if (byte == tupleDelim || byte == fieldDelim)
        {
            *cursor++ = static_cast<uint32_t>(i);
        }
    }
    return static_cast<size_t>(cursor - out);
}
#endif

/// Aggregate GB/s over input bytes for `fn(band.data(), view)` run over the whole buffer, `iterations` times.
template <typename Fn>
double measure(std::vector<uint32_t>& band, const std::string& csv, Fn&& fn, const int iterations, uint64_t& checksum, size_t& outOffsets)
{
    outOffsets = fn(band.data(), std::string_view(csv)); /// warm up + record offsets/pass
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i)
    {
        checksum += fn(band.data(), std::string_view(csv));
    }
    const auto end = std::chrono::steady_clock::now();
    const auto seconds = std::chrono::duration<double>(end - start).count();
    return static_cast<double>(csv.size()) * iterations / seconds / 1e9;
}

std::vector<uint64_t> parseList(const char* arg)
{
    std::vector<uint64_t> out;
    for (std::string s = arg;;)
    {
        const auto comma = s.find(',');
        out.push_back(std::strtoull(s.substr(0, comma).c_str(), nullptr, 10));
        if (comma == std::string::npos)
        {
            break;
        }
        s = s.substr(comma + 1);
    }
    return out;
}

}

int main(int argc, char** argv)
{
    const std::vector<uint64_t> fieldCounts = parseList((argc > 1) ? argv[1] : "1,4,16");
    const std::vector<uint64_t> fieldWidths = parseList((argc > 2) ? argv[2] : "2,4,8,16,32,64");
    const int iterations = (argc > 3) ? std::atoi(argv[3]) : 15;
    const size_t targetBytes = static_cast<size_t>((argc > 4) ? std::strtoull(argv[4], nullptr, 10) : 64) << 20;

    const auto simd = NES::SimdCsv::selectComputeBlocks();
    uint64_t checksum = 0;

    std::printf(
        "CSV indexing 2-axis scaling (~%zu MiB/cell, %d iters, %s kernel, GB/s over input bytes)\n",
        targetBytes >> 20,
        iterations,
        simdKernelName());
    std::printf("engine path = 'disp QA'. ns/row = (B/row)/GB/s. row width = N*(W+1); delim density = 1/(W+1)\n\n");
    std::printf("   N |  W | B/row | off/row | disp QA | disp noQA | fused QA | fused noQuote | ns/row(engine)\n");
    std::printf("-----+----+-------+---------+---------+-----------+----------+---------------+---------------\n");

    for (const uint64_t numFields : fieldCounts)
    {
        for (const uint64_t width : fieldWidths)
        {
            uint64_t rows = 0;
            const auto csv = makeCsv(numFields, static_cast<unsigned>(width), targetBytes, rows);
            const double bytesPerRow = static_cast<double>(csv.size()) / static_cast<double>(rows);
            std::vector<uint32_t> band(csv.size() + 64);

            size_t offsets = 0;
            size_t dummy = 0;
            const double dispQA = measure(
                band,
                csv,
                [&](uint32_t* o, std::string_view v) { return indexToBandDispatched(o, v, true, simd); },
                iterations,
                checksum,
                offsets);
            const double dispNoQA = measure(
                band,
                csv,
                [&](uint32_t* o, std::string_view v) { return indexToBandDispatched(o, v, false, simd); },
                iterations,
                checksum,
                dummy);
#if defined(__x86_64__)
            const double fusedQA = measure(
                band, csv, [&](uint32_t* o, std::string_view v) { return indexToBandFusedAvx2<true>(o, v); }, iterations, checksum, dummy);
            const double fusedNoQ = measure(
                band, csv, [&](uint32_t* o, std::string_view v) { return indexToBandFusedAvx2<false>(o, v); }, iterations, checksum, dummy);
#else
            const double fusedQA = 0.0;
            const double fusedNoQ = 0.0;
#endif
            std::printf(
                " %3llu | %2llu | %5.1f | %7.2f | %7.2f | %9.2f | %8.2f | %13.2f | %13.2f\n",
                static_cast<unsigned long long>(numFields),
                static_cast<unsigned long long>(width),
                bytesPerRow,
                static_cast<double>(offsets) / static_cast<double>(rows),
                dispQA,
                dispNoQA,
                fusedQA,
                fusedNoQ,
                bytesPerRow / dispQA);
            std::fflush(stdout);
        }
        std::printf("-----+----+-------+---------+---------+-----------+----------+---------------+---------------\n");
    }
    std::printf("(checksum %llu)\n", static_cast<unsigned long long>(checksum));
    return 0;
}
