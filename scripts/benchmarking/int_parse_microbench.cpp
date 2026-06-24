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
// Integer-parse microbench (TMP DIAGNOSTIC -- revert before merge)
// =============================================================================
//
// GATING measurement for the "fast_float integer parser" plan: is a fast_float
// integer path worth a new plugin, and WHERE does the win come from -- the digit
// loop, or the wrapper overhead (whitespace trim + 0x-hex detection + the
// full-consumption check)?
//
// The formatter's per-field parse today (DefaultInputParsers `parseFixedSized`,
// alloc-free path) is `from_chars_with_exception<T>(trimWhiteSpaces(view))`:
//   trim scan -> hex-detect -> std::from_chars(base) -> errc + full-consumption checks.
// The proposed FastInteger path is `fast_float::from_chars(p, p+len)` + a
// `ptr == p+len` full-consumption guard (no trim, no hex).
//
// Crucially: in the SELECT-key benchmark the projected field is `key` = 1-2
// DIGITS, so the digit loop is ~nothing and any win is wrapper overhead. We
// therefore sweep field WIDTH (the real data has 1-2d keys, ~7d payloads, 13d
// timestamps, and uint64 can be 20d) to see where fast_float actually helps.
//
// Variants (all parse the SAME packed field buffer; valid data never throws, so
// the happy path matches the real wrappers' work):
//   A  current : wrapStd(trimWS(sv))      -- the default formatter path today
//   B  no-trim : wrapStd(sv)              -- A minus the whitespace trim scan
//   C  fastflt : fast_float + ptr-check   -- the proposed FastInteger path
//   D  std-bare: std::from_chars, ec only -- std lower bound (no hex/consume guard)
//
// A->C is the actual proposed change; B isolates trim; C vs D isolates fast_float.
//
// Build + run in the dev container (fast_float comes from the build tree's _deps):
//   env NES_BUILD_DIR=cmake-build-release-piparse .claude/skills/nes-build/in-docker.sh bash -c \
//     'clang++-19 -O3 -std=c++20 \
//        -I {build}/_deps/fastfloat-src/include \
//        {repo}/scripts/benchmarking/int_parse_microbench.cpp -o /tmp/intparse && /tmp/intparse'
// =============================================================================

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <fast_float/fast_float.h>

namespace
{
using Clock = std::chrono::steady_clock;

/// Mirror of NES::trimWhiteSpaces cost: strip leading/trailing ASCII whitespace.
inline std::string_view trimWS(std::string_view s)
{
    size_t b = 0;
    size_t e = s.size();
    while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\n' || s[b] == '\r'))
    {
        ++b;
    }
    while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\n' || s[e - 1] == '\r'))
    {
        --e;
    }
    return s.substr(b, e - b);
}

/// Mirror of NES::from_chars_with_exception happy path (hex-detect + base + full
/// consumption check) without the throw -- returns success instead.
inline bool wrapStd(std::string_view in, uint64_t& out)
{
    const bool hex = in.size() > 2 && in[0] == '0' && (in[1] == 'x' || in[1] == 'X');
    const int base = hex ? 16 : 10;
    if (hex)
    {
        in = in.substr(2);
    }
    const auto [p, ec] = std::from_chars(in.data(), in.data() + in.size(), out, base);
    return ec == std::errc() && p == in.data() + in.size();
}

/// The proposed FastInteger path: fast_float + full-consumption guard.
inline bool wrapFF(const char* p, size_t n, uint64_t& out)
{
    const auto [ptr, ec] = fast_float::from_chars(p, p + n, out);
    return ec == std::errc() && ptr == p + n;
}

/// A packed field buffer + (offset,len) index, mirroring how the formatter reads
/// fields as slices of one contiguous raw buffer.
struct Fields
{
    std::string buf;
    std::vector<uint32_t> off;
    std::vector<uint32_t> len;

    [[nodiscard]] size_t count() const { return len.size(); }
};

Fields makeFields(uint64_t maxValue, size_t n, uint32_t seed)
{
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<uint64_t> dist(0, maxValue);
    Fields f;
    f.off.reserve(n);
    f.len.reserve(n);
    f.buf.reserve(n * 12);
    for (size_t i = 0; i < n; ++i)
    {
        const std::string s = std::to_string(dist(rng));
        f.off.push_back(static_cast<uint32_t>(f.buf.size()));
        f.len.push_back(static_cast<uint32_t>(s.size()));
        f.buf += s;
    }
    return f;
}

template <int Variant>
double runVariant(const Fields& f, uint32_t reps, uint64_t& sink)
{
    const char* base = f.buf.data();
    double best = 1e30;
    for (uint32_t rep = 0; rep < reps; ++rep)
    {
        uint64_t local = 0;
        const auto t0 = Clock::now();
        for (size_t i = 0; i < f.len.size(); ++i)
        {
            const char* p = base + f.off[i];
            const size_t n = f.len[i];
            uint64_t v = 0;
            bool ok = false;
            if constexpr (Variant == 0)
            {
                ok = wrapStd(trimWS(std::string_view(p, n)), v);
            }
            else if constexpr (Variant == 1)
            {
                ok = wrapStd(std::string_view(p, n), v);
            }
            else if constexpr (Variant == 2)
            {
                ok = wrapFF(p, n, v);
            }
            else
            {
                const auto [pp, ec] = std::from_chars(p, p + n, v);
                ok = ec == std::errc();
            }
            local += v + static_cast<uint64_t>(ok);
        }
        const auto t1 = Clock::now();
        best = std::min(best, std::chrono::duration<double>(t1 - t0).count());
        sink += local;
    }
    return best; /// min over reps == least-noisy single-thread estimate
}

void benchWidth(const char* label, uint64_t maxValue, size_t n, uint32_t reps, uint64_t& sink)
{
    const Fields f = makeFields(maxValue, n, 0xBEEF);
    double avgLen = 0;
    for (auto l : f.len)
    {
        avgLen += l;
    }
    avgLen /= static_cast<double>(f.count());

    const double a = runVariant<0>(f, reps, sink);
    const double b = runVariant<1>(f, reps, sink);
    const double c = runVariant<2>(f, reps, sink);
    const double d = runVariant<3>(f, reps, sink);
    const auto mps = [&](double secs) { return static_cast<double>(f.count()) / secs / 1e6; };
    const auto nsp = [&](double secs) { return secs / static_cast<double>(f.count()) * 1e9; };

    std::printf("%-22s (avg %.1f digits, %zu fields)\n", label, avgLen, f.count());
    std::printf("  A current  (trim+wrap) : %7.1f M/s  %6.2f ns   1.00x\n", mps(a), nsp(a));
    std::printf("  B no-trim   (wrap)     : %7.1f M/s  %6.2f ns  %5.2fx vs A\n", mps(b), nsp(b), a / b);
    std::printf(
        "  C fastfloat (proposed) : %7.1f M/s  %6.2f ns  %5.2fx vs A  (%+.0f%% vs A)\n", mps(c), nsp(c), a / c, 100.0 * (a - c) / a);
    std::printf("  D std-bare  (lower bnd): %7.1f M/s  %6.2f ns  %5.2fx vs A\n\n", mps(d), nsp(d), a / d);
}
} /// namespace

int main(int argc, char** argv)
{
    const size_t n = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 2'000'000;
    const uint32_t reps = (argc > 2) ? static_cast<uint32_t>(std::strtoul(argv[2], nullptr, 10)) : 7;
    uint64_t sink = 0;

    std::printf("=== integer-parse microbench (single thread, %zu fields, min of %u reps) ===\n", n, reps);
    std::printf("variants: A=trim+from_chars_with_exception (today)  B=no trim  C=fast_float (proposed)  D=std::from_chars bare\n\n");

    benchWidth("key   (1-2 digit)", 63ULL, n, reps, sink); /// the SELECT-key projected field
    benchWidth("payload (~7 digit)", 1'000'002ULL, n, reps, sink); /// v1/v2/v3
    benchWidth("timestamp (13 dig)", 9'999'999'999'999ULL, n, reps, sink);
    benchWidth("wide uint64 (~20d)", ~0ULL, n, reps, sink);

    std::printf("(sink %llu)\n", static_cast<unsigned long long>(sink));
    return 0;
}
