// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// "Sleepy" weak-scaling proof of concept: the same register-only compute as
// parallel_scaling_poc.cpp, but delivered through the NebulaStream query
// engine's actual concurrency structure -- P producer threads (the IO/source
// analogue) hand work to N worker threads via a std::counting_semaphore, with a
// bounded in-flight count (the availableBuffer backpressure analogue). Workers
// block on the semaphore exactly like ThreadPool::WorkerThread in
// getNextTaskBlocking. There is still ZERO memory traffic, so any drop-off
// versus the tight PoC is the block/wake + producer-handoff structure, not
// memory bandwidth and not core topology.
//
// Weak scaling: each worker consumes `tasksPerWorker` tasks, total tasks scale
// with the worker count, and the producer(s) must release a token per task. If
// a single producer cannot release tokens as fast as N workers consume them,
// workers drain the queue, block, and throughput plateaus -- mirroring a single
// IO thread feeding many workers. Sweeping producer count and producer-vs-worker
// cost ratio shows whether the knee is producer-bound (moves with #producers)
// or pure wake latency (present even with a near-free producer).
//
// Build + run in the dev container:
//   in-docker.sh bash -c 'clang++-19 -O3 -std=c++20 -pthread \
//     {repo}/scripts/benchmarking/parallel_scaling_sleepy_poc.cpp -o /tmp/sleepy && \
//     /tmp/sleepy <workIters> <prodIters> <tasksPerWorker> <numProducers> <maxWorkers> <inflight>'

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <semaphore>
#include <thread>
#include <vector>

namespace
{
/// Dependent xorshift mix, register-only (identical to the tight PoC's work()).
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

struct Params
{
    uint64_t workIters; /// compute per task on the worker (consumer) side
    uint64_t prodIters; /// compute per task on the producer side (IO/read analogue)
    uint64_t tasksPerWorker;
    unsigned numProducers;
    int inflight;
};

/// Run one (workers x producers) configuration; return wall seconds and a sink to defeat DCE.
double runConfig(unsigned workers, const Params& p, volatile uint64_t& sinkOut)
{
    const uint64_t totalTasks = static_cast<uint64_t>(workers) * p.tasksPerWorker;

    std::counting_semaphore<> tasksAvailable{0}; /// producer release / worker acquire  (== TaskQueue::tasksAvailable)
    std::counting_semaphore<> freeSlots{p.inflight}; /// bounded in-flight backpressure       (== RunningSource::availableBuffer)
    std::atomic<uint64_t> claimedByProducer{0};
    std::atomic<uint64_t> claimedByWorker{0};
    std::atomic<uint64_t> sink{0};

    std::vector<std::thread> threads;
    threads.reserve(workers + p.numProducers);

    const auto t0 = std::chrono::steady_clock::now();

    for (unsigned pi = 0; pi < p.numProducers; ++pi)
    {
        threads.emplace_back(
            [&, pi]
            {
                uint64_t local = 0;
                while (true)
                {
                    const uint64_t t = claimedByProducer.fetch_add(1);
                    if (t >= totalTasks)
                    {
                        break;
                    }
                    freeSlots.acquire(); /// block if too far ahead of the workers
                    local ^= work(0xABCDEF * (pi + 1) + t, p.prodIters);
                    tasksAvailable.release(); /// wake exactly one worker, one token per task
                }
                sink.fetch_add(local);
            });
    }

    for (unsigned wi = 0; wi < workers; ++wi)
    {
        threads.emplace_back(
            [&, wi]
            {
                uint64_t local = 0;
                while (true)
                {
                    const uint64_t c = claimedByWorker.fetch_add(1);
                    if (c >= totalTasks)
                    {
                        break;
                    }
                    tasksAvailable.acquire(); /// THE SLEEP: block here when the queue is drained
                    local ^= work(0xC0FFEE * (wi + 1) + c, p.workIters);
                    freeSlots.release();
                }
                sink.fetch_add(local);
            });
    }

    for (auto& th : threads)
    {
        th.join();
    }
    const auto t1 = std::chrono::steady_clock::now();
    sinkOut += sink.load();
    return std::chrono::duration<double>(t1 - t0).count();
}
}

int main(int argc, char** argv)
{
    Params p{
        .workIters = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 200'000ULL,
        .prodIters = (argc > 2) ? std::strtoull(argv[2], nullptr, 10) : 40'000ULL,
        .tasksPerWorker = (argc > 3) ? std::strtoull(argv[3], nullptr, 10) : 10'000ULL,
        .numProducers = (argc > 4) ? static_cast<unsigned>(std::strtoul(argv[4], nullptr, 10)) : 1U,
        .inflight = (argc > 6) ? static_cast<int>(std::strtol(argv[6], nullptr, 10)) : 64,
    };
    const unsigned maxWorkers = (argc > 5) ? static_cast<unsigned>(std::strtoul(argv[5], nullptr, 10)) : 14U;
    constexpr unsigned warmup = 1;
    constexpr unsigned reps = 3;
    volatile uint64_t sink = 0;

    std::printf(
        "sleepy weak scaling: workIters=%llu prodIters=%llu (ratio %.1f:1) tasksPerWorker=%llu producers=%u inflight=%d\n",
        static_cast<unsigned long long>(p.workIters),
        static_cast<unsigned long long>(p.prodIters),
        static_cast<double>(p.workIters) / static_cast<double>(p.prodIters),
        static_cast<unsigned long long>(p.tasksPerWorker),
        p.numProducers,
        p.inflight);

    std::vector<double> medianThpt(maxWorkers + 1, 0.0);
    for (unsigned w = 1; w <= maxWorkers; ++w)
    {
        std::vector<double> thpts;
        for (unsigned rep = 0; rep < warmup + reps; ++rep)
        {
            const double secs = runConfig(w, p, sink);
            const double giters
                = static_cast<double>(w) * static_cast<double>(p.tasksPerWorker) * static_cast<double>(p.workIters) / secs / 1e9;
            if (rep >= warmup)
            {
                thpts.push_back(giters);
            }
            std::printf("  [workers=%2u] %s: %7.3f Giter/s  (%.3fs)\n", w, rep < warmup ? "warmup" : "rep   ", giters, secs);
            std::fflush(stdout);
        }
        std::sort(thpts.begin(), thpts.end());
        medianThpt[w] = thpts[thpts.size() / 2];
    }

    const double base = medianThpt[1];
    std::printf("\n┌─────────┬──────────────┬─────────┐\n");
    std::printf("│ workers │ throughput   │ speedup │\n");
    std::printf("├─────────┼──────────────┼─────────┤\n");
    for (unsigned w = 1; w <= maxWorkers; ++w)
    {
        char thpt[32];
        std::snprintf(thpt, sizeof(thpt), "%.2f Giter/s", medianThpt[w]);
        std::printf("│ %7u │ %12s │ %5.1f× │\n", w, thpt, medianThpt[w] / base);
    }
    std::printf("└─────────┴──────────────┴─────────┘\n");
    std::printf("(checksum %llu)\n", static_cast<unsigned long long>(sink));
    return 0;
}
