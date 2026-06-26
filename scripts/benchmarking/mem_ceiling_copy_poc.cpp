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

/// Minimal standalone repro of the NebulaStream source-scaling finding: there is ONE shared memory
/// ceiling, and a per-byte COPY makes you hit it at ~1/3 the *useful* throughput.
///
///   - mode READ  (prefill analog): each thread streams its private buffer and sums it -> 1x DRAM traffic
///                                   per useful byte. Useful throughput rises to ~the memory ceiling C.
///   - mode COPY  (file/blob analog): each thread memcpy(dst,src) then sums dst -> 3x DRAM traffic per
///                                   useful byte (read src + write dst + read dst). Useful throughput
///                                   saturates the SAME ceiling C at ~C/3.
///
/// So at high thread counts you should see READ_useful ~= 3 x COPY_useful, and BOTH map to the same
/// "effective DRAM GB/s" (useful x traffic-multiplier). That is exactly prefill(47) vs copy-bound(~15)
/// on the real engine. Buffers are pre-faulted before timing, so this measures steady streaming, not
/// page faults.
///
/// Build & run (host or dev container, no deps):
///   clang++ -O3 -std=c++20 -pthread mem_ceiling_copy_poc.cpp -o /tmp/memcopy && /tmp/memcopy
///   args: [maxThreads=24] [MiB-per-thread=256] [seconds-per-point=1.0]

#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

namespace
{
std::atomic<uint64_t> g_sink{0}; ///< keeps the compiler from eliding the reads

/// One worker: loop `iters` times over a `bytes`-sized private buffer. READ sums src; COPY memcpys
/// src->dst then sums dst. Returns nothing; accumulates into g_sink so the work is observable.
void worker(bool copyMode, size_t bytes, int iters, std::barrier<>& startGate)
{
    auto* src = static_cast<uint8_t*>(std::malloc(bytes));
    auto* dst = copyMode ? static_cast<uint8_t*>(std::malloc(bytes)) : nullptr;
    std::memset(src, 1, bytes); /// pre-fault: take the page faults BEFORE timing
    if (dst != nullptr)
    {
        std::memset(dst, 2, bytes);
    }
    const size_t words = bytes / sizeof(uint64_t);
    startGate.arrive_and_wait();
    uint64_t acc = 0;
    for (int it = 0; it < iters; ++it)
    {
        if (copyMode)
        {
            std::memcpy(dst, src, bytes);
        }
        const auto* p = reinterpret_cast<const uint64_t*>(copyMode ? dst : src);
        for (size_t i = 0; i < words; ++i)
        {
            acc += p[i];
        }
    }
    g_sink.fetch_add(acc, std::memory_order_relaxed);
    std::free(src);
    std::free(dst);
}

/// Run one (mode, threads) point and return USEFUL GB/s = src-bytes-processed / wall-time.
double runPoint(bool copyMode, int threads, size_t bytesPerThread, int iters)
{
    std::barrier startGate(threads + 1);
    std::vector<std::thread> pool;
    pool.reserve(threads);
    for (int t = 0; t < threads; ++t)
    {
        pool.emplace_back(worker, copyMode, bytesPerThread, iters, std::ref(startGate));
    }
    startGate.arrive_and_wait(); /// release all workers together
    const auto t0 = std::chrono::steady_clock::now();
    for (auto& th : pool)
    {
        th.join();
    }
    const auto t1 = std::chrono::steady_clock::now();
    const double seconds = std::chrono::duration<double>(t1 - t0).count();
    const double usefulBytes = static_cast<double>(bytesPerThread) * iters * threads;
    return usefulBytes / seconds / 1e9;
}
}

int main(int argc, char** argv)
{
    const int maxThreads = argc > 1 ? std::atoi(argv[1]) : 24;
    const size_t mibPerThread = argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 256;
    const double secondsPerPoint = argc > 3 ? std::atof(argv[3]) : 1.0;
    const size_t bytesPerThread = mibPerThread * 1024 * 1024;

    /// Calibrate iters from a single-thread READ pass so each point runs ~secondsPerPoint.
    const auto c0 = std::chrono::steady_clock::now();
    runPoint(false, 1, bytesPerThread, 1);
    const double oneIterSec = std::chrono::duration<double>(std::chrono::steady_clock::now() - c0).count();
    const int iters = std::max(2, static_cast<int>(secondsPerPoint / std::max(1e-4, oneIterSec)));

    std::printf("# %zu MiB/thread, %d iters/point, sink=%llu\n", mibPerThread, iters, (unsigned long long)g_sink.load());
    std::printf("# READ_useful = 1x DRAM traffic ; COPY_useful = 3x ; effDRAM = useful x multiplier\n");
    std::printf("%8s %12s %12s %10s %14s %14s\n", "threads", "READ GB/s", "COPY GB/s", "READ/COPY", "READ effDRAM", "COPY effDRAM");
    for (int t : {1, 2, 4, 8, 12, 16, 20, 24})
    {
        if (t > maxThreads)
        {
            break;
        }
        const double rd = runPoint(false, t, bytesPerThread, iters);
        const double cp = runPoint(true, t, bytesPerThread, iters);
        std::printf("%8d %12.1f %12.1f %10.2f %14.1f %14.1f\n", t, rd, cp, rd / cp, rd * 1.0, cp * 3.0);
    }
    return 0;
}
