// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Embarrassingly-parallel weak-scaling proof of concept. Each thread runs an
// identical, fully independent, register-resident integer-mixing loop: no
// memory traffic, no locks/atomics in the hot loop, no serial fraction, no
// shared state. It is the most favourable possible workload, so its 1->8
// speedup is the hardware's parallel ceiling on this machine (turbo/DVFS and
// P/E-core heterogeneity still apply -- those are machine properties we WANT to
// keep). Compare against the NebulaStream proj-no-emit sweep to separate the
// machine ceiling from workload-specific overhead.
//
// Weak scaling: every thread does the SAME fixed number of iterations. If the
// machine scaled perfectly, wall time would stay flat as threads are added, so
// aggregate throughput = N * iters / time would grow linearly. speedup(N) =
// throughput(N) / throughput(1) = N * time(1) / time(N).
//
// Build + run inside the dev container (so the Docker VM vCPU allocation and
// scheduler match the NES benchmark):
//   in-docker.sh bash -c \
//     'clang++ -O3 -std=c++20 -pthread {repo}/scripts/benchmarking/parallel_scaling_poc.cpp \
//        -o /tmp/poc && /tmp/poc'

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

namespace
{
/// Dependent xorshift-style mix, entirely in registers. The dependency chain
/// stops the compiler from vectorising it away and makes each iteration real
/// per-core scalar work. Returns the final state so the result is observable.
uint64_t work(uint64_t seed, uint64_t iters)
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

/// Run `threads` workers, each doing `iters` iterations; return wall time in seconds.
double runOnce(unsigned threads, uint64_t iters, volatile uint64_t& sink)
{
    std::vector<uint64_t> results(threads, 0);
    std::vector<std::thread> pool;
    pool.reserve(threads);
    const auto t0 = std::chrono::steady_clock::now();
    for (unsigned t = 0; t < threads; ++t)
    {
        /// distinct seed per thread -> fully independent streams, no sharing.
        pool.emplace_back([t, iters, &results] { results[t] = work(0xC0FFEEULL * (t + 1), iters); });
    }
    for (auto& th : pool)
    {
        th.join();
    }
    const auto t1 = std::chrono::steady_clock::now();
    uint64_t acc = 0;
    for (auto r : results)
    {
        acc += r;
    }
    sink += acc; /// keep the work alive past dead-code elimination
    return std::chrono::duration<double>(t1 - t0).count();
}
}

int main(int argc, char** argv)
{
    const uint64_t iters = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 1'000'000'000ULL;
    const unsigned maxThreads = (argc > 2) ? static_cast<unsigned>(std::strtoul(argv[2], nullptr, 10)) : 8;
    constexpr unsigned warmup = 1;
    constexpr unsigned reps = 5;
    std::vector<unsigned> threadCounts;
    for (unsigned t = 1; t <= maxThreads; ++t)
    {
        threadCounts.push_back(t);
    }
    volatile uint64_t sink = 0;

    std::vector<double> medianThpt(threadCounts.size(), 0.0); /// aggregate Giter/s
    std::vector<std::pair<double, double>> spread(threadCounts.size(), {0.0, 0.0});

    for (size_t idx = 0; idx < threadCounts.size(); ++idx)
    {
        const unsigned threads = threadCounts[idx];
        std::vector<double> thpts;
        for (unsigned rep = 0; rep < warmup + reps; ++rep)
        {
            const double secs = runOnce(threads, iters, sink);
            const double giters = static_cast<double>(threads) * static_cast<double>(iters) / secs / 1e9;
            if (rep < warmup)
            {
                std::printf("  [threads=%u] warmup: %7.3f Giter/s  (%.3fs)\n", threads, giters, secs);
            }
            else
            {
                thpts.push_back(giters);
                std::printf("  [threads=%u] rep%u  : %7.3f Giter/s  (%.3fs)\n", threads, rep - warmup + 1, giters, secs);
            }
            std::fflush(stdout);
        }
        std::sort(thpts.begin(), thpts.end());
        medianThpt[idx] = thpts[thpts.size() / 2];
        spread[idx] = {thpts.front(), thpts.back()};
    }

    const double base = medianThpt[0];
    std::printf(
        "\nembarrassingly-parallel weak scaling (register-only, %llu iters/thread, median of %u reps)\n\n",
        static_cast<unsigned long long>(iters),
        reps);
    std::printf("┌─────────┬──────────────┬─────────┐\n");
    std::printf("│ threads │ throughput   │ speedup │\n");
    std::printf("├─────────┼──────────────┼─────────┤\n");
    for (size_t idx = 0; idx < threadCounts.size(); ++idx)
    {
        char thpt[32];
        std::snprintf(thpt, sizeof(thpt), "%.2f Giter/s", medianThpt[idx]);
        std::printf("│ %7u │ %12s │ %5.1f× │\n", threadCounts[idx], thpt, medianThpt[idx] / base);
    }
    std::printf("└─────────┴──────────────┴─────────┘\n");

    std::printf("\nspread (min..max Giter/s):\n");
    for (size_t idx = 0; idx < threadCounts.size(); ++idx)
    {
        std::printf("  threads=%u: %7.3f .. %7.3f\n", threadCounts[idx], spread[idx].first, spread[idx].second);
    }
    std::printf("\n(checksum %llu)\n", static_cast<unsigned long long>(sink));
    return 0;
}
