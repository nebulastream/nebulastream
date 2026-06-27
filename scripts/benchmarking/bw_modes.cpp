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

/// Measure the achievable DRAM ceiling for different ACCESS PATTERNS, to explain why a read+write engine
/// tops out below the pure-read peak. Run with AMDuProfPcm -m memory alongside for true DRAM Total/Rd/Wr.
///   read     : sum (read-only, 8 independent streams)                 -> traffic 1x, all read
///   copy_nt  : memcpy (libc uses non-temporal stores, NO write-alloc) -> traffic 2x (1R + 1W streaming)
///   copy_rfo : dst[i] = src[i] via a regular store loop the compiler keeps as normal stores
///              (forced with a per-line dependency) -> each written line is first FETCHED (read-for-
///              ownership), so traffic 3x (1R src + 1R dst-RFO + 1W dst). This is what normal engine output
///              stores do.
/// Build: clang++ -O3 -march=native -std=c++20 -pthread bw_modes.cpp -o /tmp/bwm
/// Run:   /tmp/bwm <mode> <threads> <MiB/thr> <seconds>   (mode: read|copy_nt|copy_rfo)

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
std::atomic<double> g_gbs{0.0};
std::atomic<uint64_t> g_sink{0};

void worker(const std::string& mode, size_t bytes, double seconds, std::barrier<>& gate)
{
    auto* src = static_cast<uint64_t*>(std::aligned_alloc(64, bytes));
    auto* dst = static_cast<uint64_t*>(std::aligned_alloc(64, bytes));
    std::memset(src, 1, bytes);
    std::memset(dst, 2, bytes);
    const size_t words = bytes / sizeof(uint64_t);
    const size_t chunk = words / 8;
    gate.arrive_and_wait();
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t passes = 0, sink = 0;
    while (std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count() < seconds)
    {
        if (mode == "read")
        {
            uint64_t a0 = 0, a1 = 0, a2 = 0, a3 = 0, a4 = 0, a5 = 0, a6 = 0, a7 = 0;
            for (size_t j = 0; j < chunk; ++j)
            {
                a0 += src[0 * chunk + j];
                a1 += src[1 * chunk + j];
                a2 += src[2 * chunk + j];
                a3 += src[3 * chunk + j];
                a4 += src[4 * chunk + j];
                a5 += src[5 * chunk + j];
                a6 += src[6 * chunk + j];
                a7 += src[7 * chunk + j];
            }
            sink += a0 + a1 + a2 + a3 + a4 + a5 + a6 + a7;
        }
        else if (mode == "copy_nt")
        {
            std::memcpy(dst, src, bytes);
            sink += dst[0];
        }
        else /// copy_rfo : ordinary stores -> each dst line is read-for-ownership before the write
        {
            for (size_t i = 0; i < words; ++i)
            {
                dst[i] = src[i] + 1; /// +1 so the compiler can't turn the loop into memcpy/NT
            }
            sink += dst[0];
        }
        ++passes;
    }
    const double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const double usefulBytes = static_cast<double>(bytes) * passes; /// src bytes streamed
    g_sink.fetch_add(sink, std::memory_order_relaxed);
    double cur = g_gbs.load();
    while (!g_gbs.compare_exchange_weak(cur, cur + usefulBytes / el / 1e9, std::memory_order_relaxed))
    {
    }
    std::free(src);
    std::free(dst);
}
}

int main(int argc, char** argv)
{
    const std::string mode = argc > 1 ? argv[1] : "read";
    const int threads = argc > 2 ? std::atoi(argv[2]) : 24;
    const size_t mib = argc > 3 ? std::strtoul(argv[3], nullptr, 10) : 256;
    const double seconds = argc > 4 ? std::atof(argv[4]) : 18.0;
    const size_t bytes = mib * 1024 * 1024;
    std::barrier gate(threads);
    std::vector<std::thread> pool;
    for (int t = 0; t < threads; ++t)
    {
        pool.emplace_back(worker, mode, bytes, seconds, std::ref(gate));
    }
    for (auto& th : pool)
    {
        th.join();
    }
    std::printf(
        "mode=%-8s threads=%d : useful(src)=%.1f GB/s  sink=%llu\n",
        mode.c_str(),
        threads,
        g_gbs.load(),
        (unsigned long long)g_sink.load());
    return 0;
}
