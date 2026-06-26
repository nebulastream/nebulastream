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

/// Aggressive all-core streaming kernel to measure the machine's achievable DRAM bandwidth PEAK.
/// Runs sustained for `seconds` so an external counter reader (AMDuProfPcm -m memory) can sample a clean
/// steady window -- that amd-uprof number is the ground-truth DRAM bandwidth. The program also prints its
/// own "useful" GB/s (logical bytes touched / time) for cross-check.
///   - mode read : 8 INDEPENDENT accumulators over a private buffer (breaks the loop-carried add-latency
///                 chain a single accumulator would impose), so reads are bandwidth-bound, not latency-bound.
///   - mode copy : memcpy(dst,src) -> read+write traffic (the libc memcpy uses wide / NT stores).
/// Buffers are pre-faulted before timing. Build (no deps):
///   clang++ -O3 -march=native -std=c++20 -pthread dram_peak_probe.cpp -o /tmp/peak
///   args: mode(read|copy) [threads=nproc] [MiB-per-thread=256] [seconds=20]

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace
{
std::atomic<uint64_t> g_sink{0};
std::atomic<double> g_usefulGBs{0.0};

void worker(const std::string& mode, size_t bytes, double seconds, std::barrier<>& gate)
{
    auto* src = static_cast<uint8_t*>(std::aligned_alloc(64, bytes));
    auto* dst = (mode == "copy") ? static_cast<uint8_t*>(std::aligned_alloc(64, bytes)) : nullptr;
    std::memset(src, 1, bytes);
    if (dst != nullptr)
    {
        std::memset(dst, 2, bytes);
    }
    const size_t words = bytes / sizeof(uint64_t);
    const auto* p = reinterpret_cast<const uint64_t*>(src);

    gate.arrive_and_wait();
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t passes = 0;
    uint64_t a0 = 0, a1 = 0, a2 = 0, a3 = 0, a4 = 0, a5 = 0, a6 = 0, a7 = 0;
    while (std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count() < seconds)
    {
        if (mode == "copy")
        {
            std::memcpy(dst, src, bytes);
            a0 += dst[0];
        }
        else
        {
            for (size_t i = 0; i + 8 <= words; i += 8)
            {
                a0 += p[i + 0];
                a1 += p[i + 1];
                a2 += p[i + 2];
                a3 += p[i + 3];
                a4 += p[i + 4];
                a5 += p[i + 5];
                a6 += p[i + 6];
                a7 += p[i + 7];
            }
        }
        ++passes;
    }
    const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    g_sink.fetch_add(a0 + a1 + a2 + a3 + a4 + a5 + a6 + a7, std::memory_order_relaxed);
    /// useful bytes = bytes streamed from src (1x); copy reads src once per pass too
    const double usefulGBs = static_cast<double>(bytes) * passes / elapsed / 1e9;
    double cur = g_usefulGBs.load(std::memory_order_relaxed);
    while (!g_usefulGBs.compare_exchange_weak(cur, cur + usefulGBs, std::memory_order_relaxed))
    {
    }
    std::free(src);
    std::free(dst);
}
}

int main(int argc, char** argv)
{
    const std::string mode = argc > 1 ? argv[1] : "read";
    const int threads = argc > 2 ? std::atoi(argv[2]) : static_cast<int>(std::thread::hardware_concurrency());
    const size_t mib = argc > 3 ? std::strtoul(argv[3], nullptr, 10) : 256;
    const double seconds = argc > 4 ? std::atof(argv[4]) : 20.0;
    const size_t bytes = mib * 1024 * 1024;

    std::barrier gate(threads);
    std::vector<std::thread> pool;
    pool.reserve(threads);
    for (int t = 0; t < threads; ++t)
    {
        pool.emplace_back(worker, mode, bytes, seconds, std::ref(gate));
    }
    for (auto& th : pool)
    {
        th.join();
    }
    const double useful = g_usefulGBs.load();
    const double trafficMult = (mode == "copy") ? 3.0 : 1.0; /// copy = read src + write dst + (the a0+=dst[0] is negligible); count read+write = 2x; src-stream useful x mult
    std::printf(
        "mode=%s threads=%d MiB/thr=%zu : useful=%.1f GB/s  (program estimate of DRAM = useful x %.0f = %.1f GB/s)  sink=%llu\n",
        mode.c_str(),
        threads,
        mib,
        useful,
        mode == "copy" ? 2.0 : 1.0,
        useful * (mode == "copy" ? 2.0 : 1.0),
        (unsigned long long)g_sink.load());
    (void)trafficMult;
    return 0;
}
