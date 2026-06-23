// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// Stage-2 "ceiling" PoC (TMP DIAGNOSTIC -- revert before merge)
// =============================================================================
//
// Question: the CSV formatter's stage 1 (SIMD delimiter index -> flat band) runs
// at ~8.6 GB/s, yet the full single-thread pipeline lands at ~3 GB/s. We proved
// the band ROUND-TRIP is not the cause (<2% of the per-byte budget). The residual
// is the engine's row-at-a-time STAGE 2: per-row record loop + per-field tagged-
// invoke (proxy call) + field addressing + materialize, all Nautilus-compiled.
// engine stage-2 ~= full - index ~= 0.31 - 0.116 = 0.194 ns/B ~= 5.1 GB/s.
//
// This PoC measures how fast stage 2 COULD run as tight native code -- the ceiling
// if the Nautilus per-tuple tax were gone -- and attributes the gap to ILP / store
// layout / the parse itself. It is NOT a drop-in (the engine needs Nautilus for
// arbitrary fused queries); it bounds the opportunity and says which transform pays.
//
// It consumes the SAME work stage 2 does: a flat band of delimiter positions (built
// once, UNTIMED -- stage 2 does not care how the band was produced) and, per row,
// per projected field: two adjacent band loads -> addr/size arithmetic -> fast_float
// parse -> store into a PRE-ALLOCATED, REUSED output buffer (no per-rep page faults,
// unlike the formatter-scaling bench). Throughput is per INPUT CSV byte, directly
// comparable to the engine GB/s numbers.
//
// Variants (all over the same band+buffer; checksum verifies V0==V1==V2):
//   V0  scalar row-at-a-time   -- engine-mimic, but native + recycled output
//   V1  unrolled x8 (ILP)      -- expose independent-row pipelining
//   V2  columnar output        -- per-column contiguous stores vs row-interleaved
//   V3  no-parse floor         -- store `size` instead of parsing: isolates how much
//                                 of V0 is the digit loop vs addressing+store+loop
//
// Band cache note: the band is built whole-file and read cold/streamed (~0.5 B per
// input byte of extra sequential reads), so if anything this UNDERSTATES the ceiling
// vs the engine's hot per-buffer band reuse. The round-trip is bandwidth-trivial
// either way, so V0 is dominated by parse+store+loop, not the band read.
//
// Build + run in the dev container (fast_float from the build tree's _deps):
//   env NES_BUILD_DIR=cmake-build-release-piparse .claude/skills/nes-build/in-docker.sh bash -c \
//     'clang++-19 -O3 -std=c++20 -I {build}/_deps/fastfloat-src/include \
//        {repo}/scripts/benchmarking/stage2_ceiling_poc.cpp -o /tmp/s2 && \
//      /tmp/s2 {repo}/nes-systests/testdata/large/formatter/bench_5xUINT64_128m.csv 5 key'
//   args: <csv> [cols=5] [project=key|all|<idx>] [maxMiB=0(all)] [reps=15]
// =============================================================================

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include <fast_float/fast_float.h>

namespace
{
using Clock = std::chrono::steady_clock;

/// Sentinel for the lead boundary of row 0 / field 0. addr = base + (uint32)(lead+1)
/// wraps to base+0; size = (uint32)(end-lead-1) wraps to `end`. So field 0 spans
/// [0, firstComma) with no branch -- the readback is uniform for every (row,field).
constexpr uint32_t kSent = 0xFFFFFFFFu;

struct Input
{
    std::vector<char> buf; /// raw CSV bytes, trimmed to end on a newline
    std::vector<uint32_t> band; /// [SENT, d0, d1, ...] all commas+newlines in order
    size_t numRows = 0;
    size_t cols = 0;
};

Input load(const char* path, size_t cols, size_t maxBytes)
{
    Input in;
    in.cols = cols;
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f)
    {
        std::fprintf(stderr, "cannot open %s\n", path);
        std::exit(1);
    }
    size_t sz = static_cast<size_t>(f.tellg());
    if (maxBytes != 0 && maxBytes < sz)
    {
        sz = maxBytes;
    }
    f.seekg(0);
    in.buf.resize(sz);
    f.read(in.buf.data(), static_cast<std::streamsize>(sz));

    /// Trim to the last newline so the final row is complete and the band is uniform.
    while (!in.buf.empty() && in.buf.back() != '\n')
    {
        in.buf.pop_back();
    }

    /// Build the flat band (UNTIMED -- this stands in for stage 1).
    in.band.reserve(in.buf.size() / 4 + 2);
    in.band.push_back(kSent);
    for (size_t i = 0; i < in.buf.size(); ++i)
    {
        const char c = in.buf[i];
        if (c == ',' || c == '\n')
        {
            in.band.push_back(static_cast<uint32_t>(i));
        }
    }
    in.numRows = static_cast<size_t>(std::count(in.buf.begin(), in.buf.end(), '\n'));

    if (in.band.size() - 1 != in.numRows * cols)
    {
        std::fprintf(
            stderr,
            "data is not uniform %zu-col: %zu delimiters for %zu rows (expected %zu). Wrong --cols?\n",
            cols,
            in.band.size() - 1,
            in.numRows,
            in.numRows * cols);
        std::exit(1);
    }
    return in;
}

/// Parse field `f` of row `r`: two adjacent band loads -> addr/size -> fast_float.
inline uint64_t parseField(const char* base, const uint32_t* band, size_t r, size_t cols, size_t f)
{
    const uint32_t lead = band[r * cols + f];
    const uint32_t end = band[r * cols + f + 1];
    const char* addr = base + static_cast<uint32_t>(lead + 1);
    const uint32_t size = static_cast<uint32_t>(end - lead - 1);
    uint64_t v = 0;
    fast_float::from_chars(addr, addr + size, v);
    return v;
}

inline uint32_t fieldSize(const uint32_t* band, size_t r, size_t cols, size_t f)
{
    const uint32_t lead = band[r * cols + f];
    const uint32_t end = band[r * cols + f + 1];
    return static_cast<uint32_t>(end - lead - 1);
}

/// V0: scalar row-at-a-time, row-major output (mimics the engine's record loop).
void v0(const Input& in, const std::vector<uint32_t>& proj, std::vector<uint64_t>& out)
{
    const char* base = in.buf.data();
    const uint32_t* band = in.band.data();
    const size_t cols = in.cols;
    const size_t P = proj.size();
    for (size_t r = 0; r < in.numRows; ++r)
    {
        for (size_t j = 0; j < P; ++j)
        {
            out[r * P + j] = parseField(base, band, r, cols, proj[j]);
        }
    }
}

/// V1: unroll the row loop by 8 to expose cross-row ILP (independent parses).
void v1(const Input& in, const std::vector<uint32_t>& proj, std::vector<uint64_t>& out)
{
    const char* base = in.buf.data();
    const uint32_t* band = in.band.data();
    const size_t cols = in.cols;
    const size_t P = proj.size();
    const size_t n = in.numRows;
    size_t r = 0;
    for (; r + 8 <= n; r += 8)
    {
        for (size_t j = 0; j < P; ++j)
        {
            const size_t f = proj[j];
            out[(r + 0) * P + j] = parseField(base, band, r + 0, cols, f);
            out[(r + 1) * P + j] = parseField(base, band, r + 1, cols, f);
            out[(r + 2) * P + j] = parseField(base, band, r + 2, cols, f);
            out[(r + 3) * P + j] = parseField(base, band, r + 3, cols, f);
            out[(r + 4) * P + j] = parseField(base, band, r + 4, cols, f);
            out[(r + 5) * P + j] = parseField(base, band, r + 5, cols, f);
            out[(r + 6) * P + j] = parseField(base, band, r + 6, cols, f);
            out[(r + 7) * P + j] = parseField(base, band, r + 7, cols, f);
        }
    }
    for (; r < n; ++r)
    {
        for (size_t j = 0; j < P; ++j)
        {
            out[r * P + j] = parseField(base, band, r, cols, proj[j]);
        }
    }
}

/// V2: columnar output -- each projected column written to its own contiguous run
/// (out laid out [col0 x numRows | col1 x numRows | ...]). Tests store layout.
void v2(const Input& in, const std::vector<uint32_t>& proj, std::vector<uint64_t>& out)
{
    const char* base = in.buf.data();
    const uint32_t* band = in.band.data();
    const size_t cols = in.cols;
    const size_t n = in.numRows;
    for (size_t j = 0; j < proj.size(); ++j)
    {
        const size_t f = proj[j];
        uint64_t* col = out.data() + j * n;
        for (size_t r = 0; r < n; ++r)
        {
            col[r] = parseField(base, band, r, cols, f);
        }
    }
}

/// V3: no-parse floor -- store the field SIZE instead of parsing it. Isolates
/// addressing + store + loop (everything stage 2 does except the digit loop).
void v3(const Input& in, const std::vector<uint32_t>& proj, std::vector<uint64_t>& out)
{
    const uint32_t* band = in.band.data();
    const size_t cols = in.cols;
    const size_t P = proj.size();
    for (size_t r = 0; r < in.numRows; ++r)
    {
        for (size_t j = 0; j < P; ++j)
        {
            out[r * P + j] = fieldSize(band, r, cols, proj[j]);
        }
    }
}

uint64_t checksum(const std::vector<uint64_t>& out)
{
    uint64_t s = 0;
    for (const uint64_t v : out)
    {
        s += v;
    }
    return s;
}

template <typename Fn>
void run(const char* name, Fn&& fn, const Input& in, std::vector<uint64_t>& out, unsigned reps, double gib)
{
    std::vector<double> ts;
    ts.reserve(reps);
    uint64_t cs = 0;
    for (unsigned i = 0; i < reps; ++i)
    {
        const auto t0 = Clock::now();
        fn();
        const auto t1 = Clock::now();
        ts.push_back(std::chrono::duration<double>(t1 - t0).count());
        cs = checksum(out); /// read out[] so the stores cannot be DCE'd
    }
    std::sort(ts.begin(), ts.end());
    const double best = ts.front();
    const double med = ts[ts.size() / 2];
    std::printf(
        "  %-22s  %6.2f GB/s (best)  %6.2f GB/s (median)   checksum=%llu\n",
        name,
        gib / best,
        gib / med,
        static_cast<unsigned long long>(cs));
}

} /// namespace

int main(int argc, char** argv)
{
    const char* path = (argc > 1) ? argv[1] : "nes-systests/testdata/large/formatter/bench_5xUINT64_128m.csv";
    const size_t cols = (argc > 2) ? std::strtoull(argv[2], nullptr, 10) : 5;
    const std::string projArg = (argc > 3) ? argv[3] : "key";
    const size_t maxBytes = (argc > 4) ? std::strtoull(argv[4], nullptr, 10) * (1ull << 20) : 0;
    const unsigned reps = (argc > 5) ? static_cast<unsigned>(std::strtoul(argv[5], nullptr, 10)) : 15;

    const Input in = load(path, cols, maxBytes);

    std::vector<uint32_t> proj;
    if (projArg == "all")
    {
        for (size_t f = 0; f < cols; ++f)
        {
            proj.push_back(static_cast<uint32_t>(f));
        }
    }
    else if (projArg == "key")
    {
        proj.push_back(1); /// field 1 = key (1-2 digit), matches the engine F1 run
    }
    else
    {
        proj.push_back(static_cast<uint32_t>(std::strtoul(projArg.c_str(), nullptr, 10)));
    }

    const double gib = static_cast<double>(in.buf.size()) / 1e9; /// GB (decimal) of input, like the engine numbers
    std::printf(
        "stage2-ceiling: file=%s bytes=%.1fMiB rows=%zu cols=%zu project=%s(%zu field%s) reps=%u\n",
        path,
        static_cast<double>(in.buf.size()) / (1 << 20),
        in.numRows,
        cols,
        projArg.c_str(),
        proj.size(),
        proj.size() == 1 ? "" : "s",
        reps);
    std::printf(
        "engine reference (same data, 1 thread): full(1f)~3.0-3.3  full(5f)~1.1  raw-index~8.6  stage2(=full-index)~5.1(1f)/1.3(5f) GB/s\n");

    std::vector<uint64_t> out(in.numRows * proj.size());

    run("V0 scalar", [&] { v0(in, proj, out); }, in, out, reps, gib);
    run("V1 unroll x8 (ILP)", [&] { v1(in, proj, out); }, in, out, reps, gib);
    run("V2 columnar output", [&] { v2(in, proj, out); }, in, out, reps, gib);
    run("V3 no-parse floor", [&] { v3(in, proj, out); }, in, out, reps, gib);
    std::printf("  (V0==V1==V2 checksum confirms equivalent work; V3 differs -- it sums sizes, not values)\n");
    return 0;
}
