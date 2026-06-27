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

/// Mechanistic probe for "why does real work hit a LOWER memory-bandwidth ceiling than a pure streamer?"
/// Two independent knobs:
///   ACC = number of independent load STREAMS walked in lockstep = memory-level parallelism (MLP) = the
///         number of accumulator registers live across the loop. Little's Law: achievable BW = MLP *
///         line_bytes / latency, until the memory system saturates. Too few -> request-starved (low BW);
///         too many -> register spills (pressure).
///   W   = dependent ALU ops applied to EACH loaded value before it is accumulated (a latency chain:
///         v = v*a+b). Models per-byte work (parse/index): it delays when each load is "consumed", holding
///         the load buffer longer and cutting how many loads stay in flight -> lowers effective MLP -> BW.
/// Buffers pre-faulted; measures steady streaming. Reports aggregate USEFUL GB/s (bytes streamed / time).
/// Build: clang++ -O3 -march=native -std=c++20 -pthread mlp_probe.cpp -o /tmp/mlp
/// Run:   /tmp/mlp <threads> <MiB/thr> <seconds> <ACC:1|2|4|8|16> <W>

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
std::atomic<uint64_t> g_sink{0};
std::atomic<double> g_gbs{0.0};

template <int ACC>
uint64_t kernel(const uint64_t* base, size_t chunkWords, int passes, int W)
{
    uint64_t acc[ACC];
    for (int k = 0; k < ACC; ++k)
    {
        acc[k] = 0;
    }
    for (int p = 0; p < passes; ++p)
    {
        for (size_t j = 0; j < chunkWords; ++j)
        {
#pragma unroll
            for (int k = 0; k < ACC; ++k)
            {
                uint64_t v = base[(static_cast<size_t>(k) * chunkWords) + j];
                for (int w = 0; w < W; ++w)
                {
                    v = (v * 1103515245ULL) + 12345ULL; /// dependent latency chain (load->use)
                }
                acc[k] += v;
            }
        }
    }
    uint64_t s = 0;
    for (int k = 0; k < ACC; ++k)
    {
        s += acc[k];
    }
    return s;
}

void worker(int acc, size_t bytes, double seconds, int W, std::barrier<>& gate)
{
    auto* buf = static_cast<uint64_t*>(std::aligned_alloc(64, bytes));
    std::memset(buf, 1, bytes);
    const size_t words = bytes / sizeof(uint64_t);
    /// ACC streams of size words/ACC TOGETHER cover the FULL buffer every pass -> working set is constant
    /// (= full buffer, always >> cache), so ACC varies ONLY the concurrent-stream count (MLP), not footprint.
    const size_t chunkWords = words / static_cast<size_t>(acc);
    gate.arrive_and_wait();
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t passes = 0, sink = 0;
    while (std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count() < seconds)
    {
        switch (acc)
        {
            case 1:
                sink += kernel<1>(buf, chunkWords, 1, W);
                break;
            case 2:
                sink += kernel<2>(buf, chunkWords, 1, W);
                break;
            case 4:
                sink += kernel<4>(buf, chunkWords, 1, W);
                break;
            case 8:
                sink += kernel<8>(buf, chunkWords, 1, W);
                break;
            case 16:
                sink += kernel<16>(buf, chunkWords, 1, W);
                break;
            default:
                break;
        }
        ++passes;
    }
    const double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    /// useful bytes = acc streams * chunkWords * 8 * passes
    const double bytesStreamed = static_cast<double>(acc) * chunkWords * 8.0 * passes;
    g_sink.fetch_add(sink, std::memory_order_relaxed);
    double cur = g_gbs.load();
    while (!g_gbs.compare_exchange_weak(cur, cur + bytesStreamed / el / 1e9, std::memory_order_relaxed))
    {
    }
    std::free(buf);
}
}

int main(int argc, char** argv)
{
    const int threads = argc > 1 ? std::atoi(argv[1]) : 24;
    const size_t mib = argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 256;
    const double seconds = argc > 3 ? std::atof(argv[3]) : 6.0;
    const int acc = argc > 4 ? std::atoi(argv[4]) : 8;
    const int W = argc > 5 ? std::atoi(argv[5]) : 0;
    const size_t bytes = mib * 1024 * 1024;

    std::barrier gate(threads);
    std::vector<std::thread> pool;
    for (int t = 0; t < threads; ++t)
    {
        pool.emplace_back(worker, acc, bytes, seconds, W, std::ref(gate));
    }
    for (auto& th : pool)
    {
        th.join();
    }
    std::printf("threads=%2d ACC=%2d W=%2d : %.1f GB/s  sink=%llu\n", threads, acc, W, g_gbs.load(), (unsigned long long)g_sink.load());
    return 0;
}
