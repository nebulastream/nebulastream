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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <string_view>
#include <vector>

#include <SIMDCSVKernel.hpp>
#include <SIMDCSVScan.hpp>

#if defined(__aarch64__)
    #include <arm_neon.h>
#endif

/// Self-contained micro-benchmark (no external benchmark library) for the CSV indexing phase:
/// "raw bytes -> field/tuple offsets", the work the InputFormatIndexer does before value parsing.
/// It compares three implementations that all emit the identical FieldOffsets contract:
///   1. scalar std::string_view::find loop (the original CSVInputFormatIndexer approach),
///   2. the new structural walk driven by the scalar block kernel,
///   3. the new structural walk driven by the runtime-selected SIMD block kernel (AVX2/SSE2/NEON).
/// Build (Release recommended):  cmake --build {build} --target csv-indexer-benchmark
/// Run:                          {build}/nes-input-formatters/benchmarks/csv-indexer-benchmark
namespace
{

constexpr char fieldDelim = ',';
constexpr char tupleDelim = '\n';
constexpr uint64_t numFields = 5;

/// Minimal sink mirroring FieldOffsets: stores every emitted offset (realistic memory traffic),
/// reusing its capacity across iterations.
struct BenchSink
{
    std::vector<uint32_t> offsets;

    void startSetup(uint64_t /*fields*/, size_t /*delimiterSize*/) { offsets.clear(); }

    void emplaceFieldOffset(const uint32_t offset) { offsets.push_back(offset); }

    void markNoTupleDelimiters() { }

    void markWithTupleDelimiters(uint32_t /*first*/, uint32_t /*last*/) { }
};

/// ~64 MiB of realistic 5-column CSV (no quotes; the common case).
std::string makeCsv()
{
    std::string csv;
    csv.reserve(size_t{64} << 20);
    uint32_t counter = 0;
    while (csv.size() < (size_t{64} << 20))
    {
        for (uint64_t field = 0; field < numFields; ++field)
        {
            if (field != 0)
            {
                csv += fieldDelim;
            }
            csv += std::to_string((counter * (field + 1)) % 1000000);
        }
        csv += tupleDelim;
        ++counter;
    }
    return csv;
}

/// The original CSVInputFormatIndexer scan: find the first tuple delimiter, then repeatedly find the
/// next one and the field delimiters within each tuple. Emits the same offsets as SimdCsv::indexInto.
void scalarFindScan(BenchSink& sink, const std::string_view view)
{
    sink.startSetup(numFields, 1);
    const auto firstNewline = view.find(tupleDelim);
    if (firstNewline == std::string_view::npos)
    {
        sink.markNoTupleDelimiters();
        return;
    }
    auto startIdx = firstNewline + 1;
    auto end = view.find(tupleDelim, startIdx);
    auto last = static_cast<uint32_t>(firstNewline);
    while (end != std::string_view::npos)
    {
        const auto tuple = view.substr(startIdx, end - startIdx);
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx));
        for (auto next = tuple.find(fieldDelim); next != std::string_view::npos; next = tuple.find(fieldDelim, next))
        {
            ++next;
            sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx + next));
        }
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx + tuple.size()));
        last = static_cast<uint32_t>(end);
        startIdx = end + 1;
        end = view.find(tupleDelim, startIdx);
    }
    sink.markWithTupleDelimiters(static_cast<uint32_t>(firstNewline), last);
}

/// The real CSVInputFormatIndexer default path (allow_commas_in_strings=true): newlines are found
/// with find(), but every byte of each tuple is scanned char-by-char to honor quotes. This is the
/// slow scalar path that SIMDCSV is meant to replace.
void scalarQuoteAwareScan(BenchSink& sink, const std::string_view view)
{
    sink.startSetup(numFields, 1);
    const auto firstNewline = view.find(tupleDelim);
    if (firstNewline == std::string_view::npos)
    {
        sink.markNoTupleDelimiters();
        return;
    }
    auto startIdx = firstNewline + 1;
    auto end = view.find(tupleDelim, startIdx);
    auto last = static_cast<uint32_t>(firstNewline);
    while (end != std::string_view::npos)
    {
        const auto tuple = view.substr(startIdx, end - startIdx);
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx));
        bool quoted = false;
        for (size_t i = 0; i < tuple.size(); ++i)
        {
            const char byte = tuple[i];
            if (!quoted && byte == fieldDelim)
            {
                sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx + i + 1));
            }
            quoted = quoted != (byte == '"');
        }
        sink.emplaceFieldOffset(static_cast<uint32_t>(startIdx + tuple.size()));
        last = static_cast<uint32_t>(end);
        startIdx = end + 1;
        end = view.find(tupleDelim, startIdx);
    }
    sink.markWithTupleDelimiters(static_cast<uint32_t>(firstNewline), last);
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

/// Fast path: dump every structural position (unquoted comma + newline) into a flat int32 band,
/// simdcsv-style — no per-tuple grouping, no pending buffer, no field-count validation. Returns count.
/// `out` must have capacity >= view.size() + 16.
size_t indexToBand(uint32_t* out, const std::string_view view, const bool quoteAware, const NES::SimdCsv::ComputeBlocksFn computeBlocks)
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

    bool insideQuote = quoteAware && (quoteCarry != 0);
    for (size_t i = numBlocks * 64; i < numBytes; ++i)
    {
        const char byte = data[i];
        if (byte == tupleDelim || (byte == fieldDelim && (!quoteAware || !insideQuote)))
        {
            *cursor++ = static_cast<uint32_t>(i);
        }
        if (quoteAware && byte == '"')
        {
            insideQuote = !insideQuote;
        }
    }
    return static_cast<size_t>(cursor - out);
}

#if defined(__aarch64__)
inline uint64_t benchNeonMoveMask(const uint8x16_t cmp, const uint8x16_t bitmask)
{
    const uint8x16_t anded = vandq_u8(cmp, bitmask);
    return static_cast<uint64_t>(vaddv_u8(vget_low_u8(anded))) | (static_cast<uint64_t>(vaddv_u8(vget_high_u8(anded))) << 8);
}

/// DIAGNOSTIC variant of indexToBand: the NEON kernel is inlined at compile time (no function pointer,
/// no BlockBits scratch array). Per block, compute -> field_sep -> flatten are fused with the masks
/// kept in registers. Identical work to indexToBand (all three masks computed); isolates the cost of
/// the runtime-dispatch materialization boundary.
size_t indexToBandFusedNeon(uint32_t* out, const std::string_view view, const bool quoteAware)
{
    const auto* data = reinterpret_cast<const uint8_t*>(view.data());
    const size_t numBytes = view.size();
    uint32_t* cursor = out;
    uint64_t quoteCarry = 0;
    static const uint8_t bitmaskBytes[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t bitmask = vld1q_u8(bitmaskBytes);
    const uint8x16_t vComma = vdupq_n_u8(static_cast<uint8_t>(fieldDelim));
    const uint8x16_t vNewline = vdupq_n_u8(static_cast<uint8_t>(tupleDelim));
    const uint8x16_t vQuote = vdupq_n_u8(static_cast<uint8_t>('"'));

    const size_t numBlocks = numBytes / 64;
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const uint8_t* p = data + (blk * 64);
        uint64_t comma = 0;
        uint64_t newline = 0;
        uint64_t quote = 0;
        for (unsigned j = 0; j < 4; ++j)
        {
            const uint8x16_t in = vld1q_u8(p + (j * 16));
            comma |= benchNeonMoveMask(vceqq_u8(in, vComma), bitmask) << (j * 16);
            newline |= benchNeonMoveMask(vceqq_u8(in, vNewline), bitmask) << (j * 16);
            quote |= benchNeonMoveMask(vceqq_u8(in, vQuote), bitmask) << (j * 16);
        }
        uint64_t fieldSep = 0;
        if (quoteAware)
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

    bool insideQuote = quoteAware && (quoteCarry != 0);
    for (size_t i = numBlocks * 64; i < numBytes; ++i)
    {
        const char byte = view[i];
        if (byte == tupleDelim || (byte == fieldDelim && (!quoteAware || !insideQuote)))
        {
            *cursor++ = static_cast<uint32_t>(i);
        }
        if (quoteAware && byte == '"')
        {
            insideQuote = !insideQuote;
        }
    }
    return static_cast<size_t>(cursor - out);
}

/// simdcsv's neonmovemask_bulk: reduce four 0x00/0xFF compare vectors (a full 64-byte block) to a
/// 64-bit mask with a vpaddq_u8 tree — cheaper than four separate vaddv reductions.
inline uint64_t benchNeonMoveMaskBulk(uint8x16_t p0, uint8x16_t p1, uint8x16_t p2, uint8x16_t p3, const uint8x16_t bitmask)
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

/// As indexToBandFusedNeon, but with simdcsv's vpaddq-tree movemask (one bulk reduction per mask per
/// 64-byte block instead of four vaddv reductions). Isolates the movemask-op cost.
size_t indexToBandFusedNeonBulk(uint32_t* out, const std::string_view view, const bool quoteAware)
{
    const auto* data = reinterpret_cast<const uint8_t*>(view.data());
    const size_t numBytes = view.size();
    uint32_t* cursor = out;
    uint64_t quoteCarry = 0;
    static const uint8_t bitmaskBytes[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    const uint8x16_t bitmask = vld1q_u8(bitmaskBytes);
    const uint8x16_t vComma = vdupq_n_u8(static_cast<uint8_t>(fieldDelim));
    const uint8x16_t vNewline = vdupq_n_u8(static_cast<uint8_t>(tupleDelim));
    const uint8x16_t vQuote = vdupq_n_u8(static_cast<uint8_t>('"'));

    const size_t numBlocks = numBytes / 64;
    for (size_t blk = 0; blk < numBlocks; ++blk)
    {
        const uint8_t* p = data + (blk * 64);
        const uint8x16_t b0 = vld1q_u8(p);
        const uint8x16_t b1 = vld1q_u8(p + 16);
        const uint8x16_t b2 = vld1q_u8(p + 32);
        const uint8x16_t b3 = vld1q_u8(p + 48);
        const uint64_t comma
            = benchNeonMoveMaskBulk(vceqq_u8(b0, vComma), vceqq_u8(b1, vComma), vceqq_u8(b2, vComma), vceqq_u8(b3, vComma), bitmask);
        const uint64_t newline = benchNeonMoveMaskBulk(
            vceqq_u8(b0, vNewline), vceqq_u8(b1, vNewline), vceqq_u8(b2, vNewline), vceqq_u8(b3, vNewline), bitmask);
        const uint64_t quote
            = benchNeonMoveMaskBulk(vceqq_u8(b0, vQuote), vceqq_u8(b1, vQuote), vceqq_u8(b2, vQuote), vceqq_u8(b3, vQuote), bitmask);
        uint64_t fieldSep = 0;
        if (quoteAware)
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

    bool insideQuote = quoteAware && (quoteCarry != 0);
    for (size_t i = numBlocks * 64; i < numBytes; ++i)
    {
        const char byte = view[i];
        if (byte == tupleDelim || (byte == fieldDelim && (!quoteAware || !insideQuote)))
        {
            *cursor++ = static_cast<uint32_t>(i);
        }
        if (quoteAware && byte == '"')
        {
            insideQuote = !insideQuote;
        }
    }
    return static_cast<size_t>(cursor - out);
}
#endif

/// Runs `fn(sink, csv)` `iterations` times and returns GB/s. Accumulates a checksum into `sink` usage
/// so the compiler cannot elide the work.
template <typename Fn>
double measure(Fn&& fn, const std::string& csv, const int iterations, uint64_t& checksum)
{
    BenchSink sink;
    fn(sink, csv); /// warm up + size the sink's capacity
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i)
    {
        fn(sink, csv);
        checksum += sink.offsets.size();
    }
    const auto end = std::chrono::steady_clock::now();
    const auto seconds = std::chrono::duration<double>(end - start).count();
    const auto bytes = static_cast<double>(csv.size()) * iterations;
    return bytes / seconds / 1e9;
}

/// Flat-band fast path measured at a given buffer granularity: the CSV is sliced into `chunkSize`-byte
/// pieces (0 == whole buffer in one shot) and `fn(out_ptr, chunk_view)` is called per slice, mimicking
/// the engine indexing one TupleBuffer at a time. Returns aggregate GB/s.
template <typename Fn>
double measureChunked(
    Fn&& fn, std::vector<uint32_t>& band, const std::string& csv, const size_t chunkSize, const int iterations, uint64_t& checksum)
{
    const size_t step = (chunkSize == 0) ? csv.size() : chunkSize;
    const auto runOnce = [&]
    {
        for (size_t off = 0; off < csv.size(); off += step)
        {
            const size_t len = std::min(step, csv.size() - off);
            checksum += fn(band.data(), std::string_view(csv.data() + off, len));
        }
    };
    runOnce(); /// warm up
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i)
    {
        runOnce();
    }
    const auto end = std::chrono::steady_clock::now();
    const auto seconds = std::chrono::duration<double>(end - start).count();
    const auto bytes = static_cast<double>(csv.size()) * iterations;
    return bytes / seconds / 1e9;
}

}

int main()
{
    const auto csv = makeCsv();
    constexpr int iterations = 20;
    const auto simd = NES::SimdCsv::selectComputeBlocks();
    uint64_t checksum = 0;

    const double findGbps = measure([](BenchSink& s, const std::string& c) { scalarFindScan(s, c); }, csv, iterations, checksum);
    const double quoteAwareGbps
        = measure([](BenchSink& s, const std::string& c) { scalarQuoteAwareScan(s, c); }, csv, iterations, checksum);
    const double simdQuoteGbps = measure(
        [&](BenchSink& s, const std::string& c) { NES::SimdCsv::indexInto(s, c, fieldDelim, tupleDelim, true, numFields, simd); },
        csv,
        iterations,
        checksum);
    const double simdNoQuoteGbps = measure(
        [&](BenchSink& s, const std::string& c) { NES::SimdCsv::indexInto(s, c, fieldDelim, tupleDelim, false, numFields, simd); },
        csv,
        iterations,
        checksum);

    /// Fast flat-band path (simdcsv-style branchless flatten, no per-tuple grouping / pending /
    /// validation), measured at several buffer granularities to expose per-buffer overhead.
    std::vector<uint32_t> band(csv.size() + 64);
    const auto fastFn = [&](uint32_t* out, std::string_view v) { return indexToBand(out, v, true, simd); };
    const double bandWhole = measureChunked(fastFn, band, csv, 0, iterations, checksum);
    const double band64k = measureChunked(fastFn, band, csv, 65536, iterations, checksum);
    const double band16k = measureChunked(fastFn, band, csv, 16384, iterations, checksum);
    const double band4k = measureChunked(fastFn, band, csv, 4096, iterations, checksum);

#if defined(__aarch64__)
    const auto fusedFn = [&](uint32_t* out, std::string_view v) { return indexToBandFusedNeon(out, v, true); };
    const double fusedWhole = measureChunked(fusedFn, band, csv, 0, iterations, checksum);
    const double fused4k = measureChunked(fusedFn, band, csv, 4096, iterations, checksum);
    const auto bulkFn = [&](uint32_t* out, std::string_view v) { return indexToBandFusedNeonBulk(out, v, true); };
    const double bulkWhole = measureChunked(bulkFn, band, csv, 0, iterations, checksum);
    const double bulk4k = measureChunked(bulkFn, band, csv, 4096, iterations, checksum);
#endif

    const char* kernel = simdKernelName();
    std::printf(
        "CSV indexing micro-benchmark (%zu MiB, %d iterations, %d fields/row, no quotes in data)\n",
        csv.size() >> 20,
        iterations,
        static_cast<int>(numFields));
    std::printf("  scalar baselines (current CSVInputFormatIndexer):\n");
    std::printf("    %-30s %8.2f GB/s   [allow_commas_in_strings=false]\n", "scalar find()", findGbps);
    std::printf("    %-30s %8.2f GB/s   [allow_commas_in_strings=true, DEFAULT]\n", "scalar quote-aware", quoteAwareGbps);
    std::printf("  SIMDCSV indexInto (structured FieldOffsets emission, whole buffer):\n");
    std::printf("    %-30s %8.2f GB/s   (%.2fx vs default scalar)\n", "indexInto quotes", simdQuoteGbps, simdQuoteGbps / quoteAwareGbps);
    std::printf("    %-30s %8.2f GB/s   (%.2fx vs scalar find)\n", "indexInto no quotes", simdNoQuoteGbps, simdNoQuoteGbps / findGbps);
    std::printf("  SIMDCSV flat-band fast path (%s kernel, quotes) by buffer granularity:\n", kernel);
    std::printf("    %-30s %8.2f GB/s   (%.2fx vs default scalar)\n", "whole buffer (one shot)", bandWhole, bandWhole / quoteAwareGbps);
    std::printf("    %-30s %8.2f GB/s\n", "64 KiB chunks", band64k);
    std::printf("    %-30s %8.2f GB/s\n", "16 KiB chunks", band16k);
    std::printf("    %-30s %8.2f GB/s   (engine default buffer size)\n", "4 KiB chunks", band4k);
#if defined(__aarch64__)
    std::printf("  DIAGNOSTIC — flat band, NEON inlined (no fn-ptr / no BlockBits scratch, fused):\n");
    std::printf("    %-30s %8.2f GB/s   (%.2fx vs dispatched band)\n", "vaddv movemask, whole", fusedWhole, fusedWhole / bandWhole);
    std::printf("    %-30s %8.2f GB/s   (%.2fx vs dispatched band)\n", "vaddv movemask, 4 KiB", fused4k, fused4k / band4k);
    std::printf("    %-30s %8.2f GB/s   (%.2fx vs dispatched band)\n", "vpaddq-bulk movemask, whole", bulkWhole, bulkWhole / bandWhole);
    std::printf("    %-30s %8.2f GB/s   (%.2fx vs dispatched band)\n", "vpaddq-bulk movemask, 4 KiB", bulk4k, bulk4k / band4k);
#endif
    std::printf("(checksum %llu)\n", static_cast<unsigned long long>(checksum));
    return 0;
}
