// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Worker-side MEMORY scaling PoC (TMP DIAGNOSTIC -- revert before merge).
//
// The register-only PoCs (parallel_scaling_poc / _sleepy) proved the cores are
// symmetric (linear to 14x) and the producer->worker semaphore handoff is fine.
// Yet the real CSV worker stage only reaches ~1.7x at 2 threads and walls at
// ~6-8 GB/s -- and that wall is FAR below the M5 Max DRAM bandwidth (>100 GB/s),
// so it is not raw DRAM saturation. This PoC replays the worker's actual
// per-tuple MEMORY traffic, with each sub-pattern toggleable, to localize WHICH
// access pattern stops scaling at 2 threads -- turning "memory contention by
// elimination" into a direct measurement.
//
// Per buffer (the flat-band SELECT-key worker), mirrored here:
//   STREAM : read every raw byte once          (the SIMD index bitmask scan)
//   BANDW  : + write `fields` uint32 / row      (the flat-band emission)
//   BANDR  : + read band[i],band[i+1] (dep.) + read the projected field bytes
//                                                (the parse: dependent loads)
//   FULL   : + write `proj`*8 bytes / row        (materialize the output tuple)
// Each mode is cumulative, so the FIRST mode whose 2-thread multiplier collapses
// to ~1.7x is the culprit pattern. The STREAM arm also calibrates the achievable
// read-bandwidth ceiling (prove 6-8 GB/s != DRAM). Sweeping the raw-buffer pool
// size (--pool) moves the worker's read working-set from L1/L2-resident to
// DRAM-cold, separating shared-cache contention from DRAM bandwidth.
//
// Weak scaling: each worker processes the same bytes regardless of thread count,
// so ideal aggregate throughput is linear in threads; any shortfall is on-chip
// memory contention between workers (input is per-thread, never shared).
//
// Build + run in the dev container:
//   in-docker.sh bash -c 'clang++-19 -O3 -std=c++20 -pthread \
//     {repo}/scripts/benchmarking/worker_mem_scaling_poc.cpp -o /tmp/memscale && \
//     /tmp/memscale'   # all knobs have engine-matched defaults; see Params below

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

namespace
{
struct Params
{
    uint64_t bufBytes; /// raw buffer size (engine operator_buffer_size)
    uint64_t rowBytes; /// avg bytes per CSV row
    uint32_t fields; /// structural positions emitted per row (== band entries/row)
    uint32_t proj; /// projected fields materialized per row
    uint64_t bytesPerWorker; /// raw input each worker streams (weak scaling)
    uint32_t poolBufs; /// per-worker pool of raw buffers reused round-robin (working-set knob)
    uint32_t maxWorkers;
};

enum Mode
{
    STREAM = 0, /// read only
    BANDW = 1, /// + band write
    BANDR = 2, /// + band read (dependent) + projected-field read
    FULL = 3 /// + materialize write
};

const char* modeName(int m)
{
    switch (m)
    {
        case STREAM:
            return "STREAM (read)";
        case BANDW:
            return "+BANDW (read+bandwr)";
        case BANDR:
            return "+BANDR (+dep band rd+field rd)";
        default:
            return "FULL  (+materialize)";
    }
}

std::atomic<uint64_t> g_globalSink{0};
std::atomic<int> g_startGate{0};

/// Process one buffer with the given cumulative access pattern. Returns a checksum (defeats DCE).
inline uint64_t processBuffer(int mode, const uint8_t* raw, uint64_t bufBytes, uint32_t* band, uint8_t* out, const Params& p)
{
    const uint64_t rows = bufBytes / p.rowBytes;
    uint64_t acc = 0;

    /// STREAM: read every raw byte once with 8 INDEPENDENT lanes so the loop is bandwidth/ILP-bound
    /// (not a single serial XOR chain) -- this is what tests DRAM/L2 bandwidth contention across workers.
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
    if (mode == STREAM)
    {
        return acc;
    }

    /// BANDW: emit `fields` monotone structural positions per row into the flat band (sequential write).
    uint32_t pos = 0;
    uint64_t bandIdx = 0;
    for (uint64_t r = 0; r < rows; ++r)
    {
        const uint32_t rowStart = static_cast<uint32_t>(r * p.rowBytes);
        const uint32_t step = static_cast<uint32_t>(p.rowBytes) / p.fields;
        for (uint32_t f = 0; f < p.fields; ++f)
        {
            band[bandIdx++] = rowStart + (f + 1) * step - 1; /// comma/newline position
        }
        (void)pos;
    }
    if (mode == BANDW)
    {
        return acc ^ band[bandIdx ? bandIdx - 1 : 0];
    }

    /// BANDR: parse the projected field(s): dependent band reads + a scattered raw read of the field bytes.
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
            if (mode == FULL)
            {
                std::memcpy(out + (r * p.proj + f) * 8, &v, 8); /// materialize the output tuple
            }
        }
    }
    return acc;
}

double runConfig(int mode, uint32_t workers, const Params& p)
{
    const uint64_t buffersPerWorker = p.bytesPerWorker / p.bufBytes;
    const uint64_t rowsPerBuf = p.bufBytes / p.rowBytes;
    const uint64_t bandBytesPerBuf = rowsPerBuf * p.fields * sizeof(uint32_t);
    const uint64_t outBytesPerBuf = rowsPerBuf * p.proj * 8;

    g_startGate.store(0);
    std::vector<std::thread> threads;
    std::vector<double> secsPer(workers, 0.0);
    threads.reserve(workers);

    for (uint32_t wi = 0; wi < workers; ++wi)
    {
        threads.emplace_back(
            [&, wi]
            {
                /// Per-worker raw-buffer pool (reused round-robin == bounded inflight set). Never shared
                /// between workers, so any cross-worker contention is the memory subsystem, not the data.
                std::vector<std::vector<uint8_t>> pool(p.poolBufs);
                for (auto& b : pool)
                {
                    b.assign(p.bufBytes, static_cast<uint8_t>('0' + (wi & 7)));
                }
                std::vector<uint32_t> band(bandBytesPerBuf / sizeof(uint32_t) + p.fields, 0);
                std::vector<uint8_t> out(outBytesPerBuf + 64, 0);

                g_startGate.fetch_add(1);
                while (g_startGate.load() < static_cast<int>(workers))
                {
                    /// spin until all workers are ready, so wall == steady contended window
                }
                const auto t0 = std::chrono::steady_clock::now();
                uint64_t acc = 0;
                for (uint64_t k = 0; k < buffersPerWorker; ++k)
                {
                    acc ^= processBuffer(mode, pool[k % p.poolBufs].data(), p.bufBytes, band.data(), out.data(), p);
                }
                const auto t1 = std::chrono::steady_clock::now();
                secsPer[wi] = std::chrono::duration<double>(t1 - t0).count();
                g_globalSink.fetch_add(acc);
            });
    }
    for (auto& th : threads)
    {
        th.join();
    }
    return *std::max_element(secsPer.begin(), secsPer.end()); /// wall == slowest worker
}

double medianGBs(int mode, uint32_t workers, const Params& p, uint32_t reps, uint32_t warmup)
{
    const double totalBytes = static_cast<double>(workers) * static_cast<double>(p.bytesPerWorker);
    std::vector<double> gbs;
    for (uint32_t rep = 0; rep < warmup + reps; ++rep)
    {
        const double secs = runConfig(mode, workers, p);
        if (rep >= warmup)
        {
            gbs.push_back(totalBytes / secs / 1e9);
        }
    }
    std::sort(gbs.begin(), gbs.end());
    return gbs[gbs.size() / 2];
}
}

int main(int argc, char** argv)
{
    Params p{
        .bufBytes = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 131072ULL,
        .rowBytes = (argc > 2) ? std::strtoull(argv[2], nullptr, 10) : 40ULL,
        .fields = (argc > 3) ? static_cast<uint32_t>(std::strtoul(argv[3], nullptr, 10)) : 5U,
        .proj = (argc > 4) ? static_cast<uint32_t>(std::strtoul(argv[4], nullptr, 10)) : 1U,
        .bytesPerWorker = (argc > 5) ? std::strtoull(argv[5], nullptr, 10) : (512ULL << 20),
        .poolBufs = (argc > 6) ? static_cast<uint32_t>(std::strtoul(argv[6], nullptr, 10)) : 64U,
        .maxWorkers = (argc > 7) ? static_cast<uint32_t>(std::strtoul(argv[7], nullptr, 10)) : 8U,
    };
    constexpr uint32_t reps = 3;
    constexpr uint32_t warmup = 1;
    const std::vector<uint32_t> threadCounts = {1, 2, 3, 4, 6, 8, 10, 12, 14};

    std::printf(
        "worker_mem_scaling: buf=%lluKiB row=%lluB fields=%u proj=%u perWorker=%lluMiB pool=%u (workingset=%lluKiB/worker)\n",
        static_cast<unsigned long long>(p.bufBytes >> 10),
        static_cast<unsigned long long>(p.rowBytes),
        p.fields,
        p.proj,
        static_cast<unsigned long long>(p.bytesPerWorker >> 20),
        p.poolBufs,
        static_cast<unsigned long long>((p.poolBufs * p.bufBytes) >> 10));

    for (int mode = STREAM; mode <= FULL; ++mode)
    {
        std::printf("\n=== mode %d: %s ===\n", mode, modeName(mode));
        std::printf("  threads |   GB/s | speedup | per-thread GB/s\n");
        double base = 0.0;
        for (uint32_t w : threadCounts)
        {
            if (w > p.maxWorkers)
            {
                break;
            }
            const double gbs = medianGBs(mode, w, p, reps, warmup);
            if (w == 1)
            {
                base = gbs;
            }
            std::printf("  %7u | %6.2f | %6.2fx | %6.2f\n", w, gbs, gbs / base, gbs / w);
            std::fflush(stdout);
        }
    }
    std::printf("\n(checksum %llu)\n", static_cast<unsigned long long>(g_globalSink.load()));
    return 0;
}
