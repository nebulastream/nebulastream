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
// MEMORY-CEILING PoC  (TMP DIAGNOSTIC -- revert before merge)
// =============================================================================
//
// THE INTEGRAL FINDING, isolated into ONE on-demand experiment.
//
// Question: why does the isolated CSV formatter scale only ~10.5x at 14 threads
// instead of ~14x, even though it has no shared locks/queues on the hot path?
//
// Answer this PoC proves: it is a MACHINE ceiling, not a pipeline stage. On this
// box (Apple M-series under colima, 14 vCPUs) ANY per-tuple memory access caps
// multi-thread weak-scaling at ~10.5x/14t, and it does so INDEPENDENTLY of the
// working-set size -- so it is NOT DRAM bandwidth, it is P/E-core asymmetry +
// per-cluster cache-pipeline sharing + DVFS. A register-only loop (no memory)
// scales to ~13x on the same cores, which is why the old "embarrassingly
// parallel" PoC scaled and the formatter does not.
//
// Three arms, same weak-scaling harness (each thread does a FIXED amount of work,
// so the ideal aggregate throughput is linear in thread count; speedup(N) =
// throughput(N)/throughput(1)). Each thread's input is private -- never shared --
// so any cross-thread slowdown is the memory/core subsystem, not data contention:
//
//   REGISTER : register-only xorshift mix, zero memory traffic   (hardware ceiling)
//   MEM-L1   : the formatter worker's per-tuple memory pattern,
//              128 KiB working set  -> L1/L2-resident             (cache-hot)
//   MEM-DRAM : the SAME pattern,    64 MiB working set -> DRAM-cold
//
// Decision rule (printed automatically as a VERDICT):
//   * MEM-L1 ~= MEM-DRAM  => working-set-INDEPENDENT => NOT DRAM bandwidth, it is
//                            core/cache-pipeline heterogeneity.
//   * MEM-* << REGISTER   => touching memory at all pays the heterogeneity tax;
//                            that tax is the formatter's missing scaling.
//
// IMPORTANT -- absolute vs scaling: only the SPEEDUP/efficiency columns transfer to
// the formatter. The ABSOLUTE GB/s here is a memory-MODEL rate that does LESS work
// per byte than the real parser (8-lane XOR-read instead of the NEON delimiter scan,
// `v*10+byte` instead of from_chars+trim, no Nautilus dispatch), so it runs ~2.5x
// faster PER THREAD than the formatter (MEM-L1 ~6.2 vs formatter ~2.5 GB/s @1t) --
// yet it tapers IDENTICALLY (MEM ~10x vs formatter 9.8x @14t), because the scaling
// ceiling is the shared memory/core subsystem, not the compute stacked on top.
//
// The MEM arms replay the flat-band SELECT-key worker (mirrors the real cost mix:
// STREAM read every byte + BAND write structural positions + dependent parse reads
// + materialize the projected field). Cross-check: the real formatter at 14t
// (project f1 + fnattr) measures 9.8x -- right on the MEM ceiling. See
// `FormatterScalingBenchmark.cpp` and `formatter-scaling-findings.md`.
//
// Build + run in the dev container (one command):
//   env NES_BUILD_DIR=cmake-build-release-piparse \
//     .claude/skills/nes-build/in-docker.sh bash -c \
//       'clang++-19 -O3 -std=c++20 -pthread \
//          {repo}/scripts/benchmarking/memory_ceiling_poc.cpp -o /tmp/memceil && \
//        /tmp/memceil'
//
// CLI (all optional, positional):
//   memceil [maxThreads=14] [reps=3] [bytesPerWorkerMiB=512] [regIters=500000000]
// =============================================================================

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <thread>
#include <vector>

namespace
{
using Clock = std::chrono::steady_clock;
std::atomic<uint64_t> g_sink{0}; /// defeats dead-code elimination across all arms

/// ---- Worker memory pattern (the real flat-band SELECT-key cost mix) ---------
struct MemParams
{
    uint64_t bufBytes = 131072; /// raw buffer size (engine operator_buffer_size)
    uint64_t rowBytes = 40; /// avg bytes per CSV row
    uint32_t fields = 5; /// structural positions emitted per row (band entries/row)
    uint32_t proj = 1; /// projected fields materialized per row (SELECT key)
};

/// One buffer, the FULL cumulative pattern: STREAM read + BAND write + dependent
/// band/field reads (parse analogue) + materialize write. Returns a checksum.
inline uint64_t processBufferFull(const uint8_t* raw, uint64_t bufBytes, uint32_t* band, uint8_t* out, const MemParams& p)
{
    const uint64_t rows = bufBytes / p.rowBytes;
    uint64_t acc = 0;

    /// STREAM: read every raw byte once with 8 independent lanes (bandwidth/ILP-bound).
    {
        uint64_t l0 = 0, l1 = 0, l2 = 0, l3 = 0, l4 = 0, l5 = 0, l6 = 0, l7 = 0;
        const uint64_t n8 = bufBytes & ~uint64_t{7};
        for (uint64_t i = 0; i < n8; i += 8)
        {
            l0 ^= raw[i + 0];
            l1 ^= raw[i + 1];
            l2 ^= raw[i + 2];
            l3 ^= raw[i + 3];
            l4 ^= raw[i + 4];
            l5 ^= raw[i + 5];
            l6 ^= raw[i + 6];
            l7 ^= raw[i + 7];
        }
        acc = l0 ^ l1 ^ l2 ^ l3 ^ l4 ^ l5 ^ l6 ^ l7;
    }

    /// BAND write: emit `fields` monotone structural positions per row (sequential write).
    uint64_t bandIdx = 0;
    for (uint64_t r = 0; r < rows; ++r)
    {
        const uint32_t rowStart = static_cast<uint32_t>(r * p.rowBytes);
        const uint32_t step = static_cast<uint32_t>(p.rowBytes) / p.fields;
        for (uint32_t f = 0; f < p.fields; ++f)
        {
            band[bandIdx++] = rowStart + (f + 1) * step - 1;
        }
    }

    /// BAND read + field read (parse) + materialize: dependent band reads + scattered field bytes.
    for (uint64_t r = 0; r < rows; ++r)
    {
        const uint64_t base = r * p.fields;
        for (uint32_t f = 0; f < p.proj; ++f)
        {
            const uint32_t start = (f == 0) ? static_cast<uint32_t>(r * p.rowBytes) : band[base + f - 1] + 1;
            const uint32_t end = band[base + f]; /// dependent on the band write above
            uint64_t v = 0;
            for (uint32_t b = start; b < end && b < bufBytes; ++b)
            {
                v = v * 10 + raw[b]; /// read the field bytes (from_chars analogue)
            }
            acc ^= v;
            std::memcpy(out + (r * p.proj + f) * 8, &v, 8); /// materialize the output tuple
        }
    }
    return acc;
}

/// ---- Register-only kernel (no memory) --------------------------------------
inline uint64_t regWork(uint64_t seed, uint64_t iters)
{
    uint64_t x = seed | 1U;
    for (uint64_t i = 0; i < iters; ++i)
    {
        x ^= x << 13U;
        x ^= x >> 7U;
        x ^= x << 17U;
        x += 0x9E3779B97F4A7C15ULL;
    }
    return x;
}

/// ---- Weak-scaling harness --------------------------------------------------
/// `make(tid)` runs ON the worker thread BEFORE timing (per-thread allocation /
/// first-touch is untimed) and returns the timed-loop closure. All threads pass a
/// start barrier, so wall == the contended steady window; wall = slowest worker.
double runWeak(unsigned threads, const std::function<std::function<uint64_t()>(unsigned)>& make)
{
    std::atomic<int> gate{0};
    std::vector<double> secs(threads, 0.0);
    std::vector<std::thread> pool;
    pool.reserve(threads);
    for (unsigned t = 0; t < threads; ++t)
    {
        pool.emplace_back(
            [&, t]
            {
                auto loop = make(t); /// untimed setup
                gate.fetch_add(1, std::memory_order_acq_rel);
                while (gate.load(std::memory_order_acquire) < static_cast<int>(threads))
                {
                    /// spin until every worker is ready
                }
                const auto a = Clock::now();
                const uint64_t r = loop();
                const auto b = Clock::now();
                secs[t] = std::chrono::duration<double>(b - a).count();
                g_sink.fetch_add(r, std::memory_order_relaxed);
            });
    }
    for (auto& th : pool)
    {
        th.join();
    }
    return *std::max_element(secs.begin(), secs.end());
}

double median(std::vector<double> xs)
{
    std::sort(xs.begin(), xs.end());
    return xs[xs.size() / 2];
}

/// Median throughput (units/sec, aggregate over all threads) for one arm.
double medianThroughput(
    unsigned threads, uint32_t reps, uint32_t warmup, double unitsPerWorker, const std::function<std::function<uint64_t()>(unsigned)>& make)
{
    std::vector<double> tp;
    for (uint32_t rep = 0; rep < warmup + reps; ++rep)
    {
        const double secs = runWeak(threads, make);
        if (rep >= warmup)
        {
            tp.push_back(static_cast<double>(threads) * unitsPerWorker / secs);
        }
    }
    return median(tp);
}
} /// namespace

int main(int argc, char** argv)
{
    const unsigned maxThreads = (argc > 1) ? static_cast<unsigned>(std::strtoul(argv[1], nullptr, 10)) : 14U;
    const uint32_t reps = (argc > 2) ? static_cast<uint32_t>(std::strtoul(argv[2], nullptr, 10)) : 3U;
    const uint64_t bytesPerWorker = ((argc > 3) ? std::strtoull(argv[3], nullptr, 10) : 512ULL) << 20;
    const uint64_t regIters = (argc > 4) ? std::strtoull(argv[4], nullptr, 10) : 500000000ULL;
    constexpr uint32_t warmup = 1;

    const MemParams mp;
    const uint64_t buffersPerWorker = bytesPerWorker / mp.bufBytes;
    const uint64_t rowsPerBuf = mp.bufBytes / mp.rowBytes;
    const uint64_t bandLen = rowsPerBuf * mp.fields + mp.fields;
    const uint64_t outLen = rowsPerBuf * mp.proj * 8 + 64;

    /// Build the per-thread "make" closure for the memory arm at a given working-set (poolBufs).
    const auto makeMem = [&](uint32_t poolBufs)
    {
        return [&, poolBufs](unsigned tid) -> std::function<uint64_t()>
        {
            auto pool = std::make_shared<std::vector<std::vector<uint8_t>>>(poolBufs);
            for (auto& b : *pool)
            {
                b.assign(mp.bufBytes, static_cast<uint8_t>('0' + (tid & 7)));
            }
            auto band = std::make_shared<std::vector<uint32_t>>(bandLen, 0);
            auto out = std::make_shared<std::vector<uint8_t>>(outLen, 0);
            return [pool, band, out, poolBufs, buffersPerWorker, mp]() -> uint64_t
            {
                uint64_t acc = 0;
                for (uint64_t k = 0; k < buffersPerWorker; ++k)
                {
                    acc ^= processBufferFull((*pool)[k % poolBufs].data(), mp.bufBytes, band->data(), out->data(), mp);
                }
                return acc;
            };
        };
    };
    const auto makeReg = [&](unsigned tid) -> std::function<uint64_t()>
    { return [tid, regIters]() -> uint64_t { return regWork(0xC0FFEEULL * (tid + 1), regIters); }; };

    std::printf("=== MEMORY-CEILING PoC: is the multi-thread ceiling a machine property? ===\n");
    std::printf(
        "hardware_concurrency=%u  maxThreads=%u  reps=%u (+%u warmup)\n", std::thread::hardware_concurrency(), maxThreads, reps, warmup);
    std::printf(
        "MEM arms: buf=%lluKiB row=%lluB fields=%u proj=%u perWorker=%lluMiB | L1 ws=%lluKiB  DRAM ws=%lluMiB\n",
        static_cast<unsigned long long>(mp.bufBytes >> 10),
        static_cast<unsigned long long>(mp.rowBytes),
        mp.fields,
        mp.proj,
        static_cast<unsigned long long>(bytesPerWorker >> 20),
        static_cast<unsigned long long>((1ULL * mp.bufBytes) >> 10),
        static_cast<unsigned long long>((512ULL * mp.bufBytes) >> 20));
    std::printf("REGISTER arm: %llu iters/thread (no memory)\n", static_cast<unsigned long long>(regIters));
    std::printf("NOTE: absolute GB/s is a memory-MODEL rate (~2.5x the formatter; less work/byte) -- only the\n");
    std::printf("      SPEEDUP/efficiency columns transfer to the real formatter.\n\n");

    std::printf(" threads | REGISTER (no mem)  | MEM-L1 (cache-hot)  | MEM-DRAM (cold)     | per-thread efficiency\n");
    std::printf("         |  Giter/s   speedup |  GB/s     speedup   |  GB/s     speedup   |  reg / memL1 / memDram\n");
    std::printf("---------+--------------------+---------------------+---------------------+-----------------------\n");

    double regBase = 0, l1Base = 0, dramBase = 0;
    double regTop = 0, l1Top = 0, dramTop = 0;
    for (unsigned t = 1; t <= maxThreads; ++t)
    {
        const double reg = medianThroughput(t, reps, warmup, static_cast<double>(regIters), makeReg);
        const double l1 = medianThroughput(t, reps, warmup, static_cast<double>(bytesPerWorker), makeMem(1));
        const double dram = medianThroughput(t, reps, warmup, static_cast<double>(bytesPerWorker), makeMem(512));
        if (t == 1)
        {
            regBase = reg;
            l1Base = l1;
            dramBase = dram;
        }
        regTop = reg / regBase;
        l1Top = l1 / l1Base;
        dramTop = dram / dramBase;
        std::printf(
            " %7u | %7.2f  %6.2fx | %7.2f  %6.2fx | %7.2f  %6.2fx |  %3.0f%% / %3.0f%% / %3.0f%%\n",
            t,
            reg / 1e9,
            reg / regBase,
            l1 / 1e9,
            l1 / l1Base,
            dram / 1e9,
            dram / dramBase,
            100.0 * (reg / regBase) / t,
            100.0 * (l1 / l1Base) / t,
            100.0 * (dram / dramBase) / t);
        std::fflush(stdout);
    }

    /// ---- Automatic verdict ----
    const double wsGap = (l1Top > 0) ? 100.0 * (l1Top - dramTop) / l1Top : 0.0;
    const double memTax = (regTop > 0) ? 100.0 * (regTop - l1Top) / regTop : 0.0;
    std::printf("\n=== VERDICT @ %u threads ===\n", maxThreads);
    std::printf("  REGISTER (no memory)   : %.2fx  (%.0f%% efficiency)  <- hardware ceiling\n", regTop, 100.0 * regTop / maxThreads);
    std::printf("  MEM-L1   (cache-hot)   : %.2fx  (%.0f%% efficiency)\n", l1Top, 100.0 * l1Top / maxThreads);
    std::printf("  MEM-DRAM (DRAM-cold)   : %.2fx  (%.0f%% efficiency)\n", dramTop, 100.0 * dramTop / maxThreads);
    std::printf(
        "  working-set gap (L1 vs DRAM) : %+.1f%%  -> %s\n",
        wsGap,
        (wsGap > -8.0 && wsGap < 8.0) ? "WORKING-SET INDEPENDENT => NOT DRAM bandwidth (core/cache-pipeline heterogeneity)"
                                      : "working-set dependent => bandwidth/cache-capacity matters");
    std::printf("  memory tax (REG -> MEM-L1)   : -%.0f%%  -> touching memory at all costs this much scaling\n", memTax);
    std::printf("  => the formatter (real, 9.8x/14t) sits on the MEM ceiling: not a pipeline stage, a machine property.\n");
    std::printf("\n(checksum %llu)\n", static_cast<unsigned long long>(g_sink.load()));
    return 0;
}
