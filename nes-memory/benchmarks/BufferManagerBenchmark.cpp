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

/// Self-contained microbenchmark (no google-benchmark dependency, which is not part of the vcpkg
/// manifest) comparing the ways a stream engine can serve a *variable-sized* buffer request. It is the
/// performance gate for the size-class work and, in addition, the head-to-head comparison of the three
/// design alternatives from the design doc:
///
///   A1 SizeClass    -- segregated power-of-two pooled size classes (what we built).
///   A2 ComposeFixed -- keep one fixed buffer size, satisfy a large request by chaining ceil(S/block)
///                      fixed buffers (the DuckDB "compose fixed blocks" route).
///   A3 VmCache      -- reserve one big anonymous mmap arena and hand out page-rounded slices; the
///                      resident set is what the OS faults in. Two operating points:
///                        reuse   -- keep freed slices resident (fast, high resident footprint),
///                        reclaim -- madvise(DONTNEED) every free (bounded resident, syscall per op).
///
/// Plus the Unpooled baseline (the legacy variable-sized path) for reference.
///
/// This is an *allocator-level* comparison: it measures alloc + touch(S bytes) + free latency and the
/// internal fragmentation each scheme carries. It is deliberately NOT an end-to-end integration -- the
/// point is to expose the intrinsic cost of each allocation strategy (A2 does N pops for a large
/// request; A3 faults pages on first touch; A1/Unpooled do one pop) under thread scaling.
///
/// Each iteration allocates one buffer, writes every byte of the requested size (so A3's page faults
/// are paid), and immediately releases it (RAII), so a pool of N buffers is never exhausted by up to N
/// concurrent threads.

/// NOLINTBEGIN(readability-magic-numbers,readability-identifier-length,google-build-using-namespace,cppcoreguidelines-narrowing-conversions,misc-include-cleaner)

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sys/mman.h>
#include <folly/MPMCQueue.h>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace
{
using namespace NES;

constexpr uint32_t DEFAULT_SIZE = 8192;
constexpr uint32_t NUM_BUFFERS = 16384;
constexpr uint32_t COMPOSE_BLOCK = 4096; /// A2's single fixed block size (page-sized, DuckDB-style).
constexpr size_t PAGE = 4096;
/// Two request sizes: SMALL fits in one block/class (shows small-request overhead), LARGE spans many
/// blocks (shows A2's chaining cost and A1's power-of-two round-up).
constexpr size_t SMALL_SIZE = 1024;
constexpr size_t LARGE_SIZE = 49152; /// 48 KiB
constexpr size_t ITERATIONS_PER_THREAD = 1'000'000;

size_t nextPow2(size_t n)
{
    size_t p = 1;
    while (p < n)
    {
        p <<= 1;
    }
    return p;
}
size_t roundUp(size_t n, size_t m)
{
    return ((n + m - 1) / m) * m;
}
void touch(void* p, size_t n)
{
    std::memset(p, 0xAB, n); /// fault + dirty every page of the request, like a real operator would.
}

SizeClassConfig eagerSizeClasses()
{
    SizeClassConfig config{.minClassSize = 512, .maxClassSize = 65536};
    config.policy = BufferProvisioningPolicy::EagerPerClass;
    config.buffersPerClass = NUM_BUFFERS;
    return config;
}

/// A3: one anonymous mmap arena carved into fixed page-rounded slices, handed out through a lock-free
/// queue (same machinery the pool uses). `reclaim` madvises each freed slice away so the resident set
/// stays near zero at the cost of a syscall + re-fault per op.
class VmCache
{
public:
    VmCache(size_t sliceBytes, size_t slices, bool reclaim)
        : sliceBytes(roundUp(sliceBytes, PAGE)), reclaim(reclaim), free(slices)
    {
        arenaBytes = this->sliceBytes * slices;
        arena = static_cast<uint8_t*>(
            mmap(nullptr, arenaBytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0));
        for (size_t i = 0; i < slices; ++i)
        {
            free.blockingWrite(arena + i * this->sliceBytes);
        }
    }
    ~VmCache() { munmap(arena, arenaBytes); }

    uint8_t* alloc()
    {
        uint8_t* p = nullptr;
        free.blockingRead(p);
        return p;
    }
    void release(uint8_t* p)
    {
        if (reclaim)
        {
            madvise(p, sliceBytes, MADV_DONTNEED); /// return physical pages to the OS.
        }
        free.blockingWrite(p);
    }

private:
    size_t sliceBytes;
    size_t arenaBytes;
    bool reclaim;
    uint8_t* arena;
    folly::MPMCQueue<uint8_t*> free;
};

/// Runs `work` (one alloc+touch+free) ITERATIONS_PER_THREAD times on each of `numThreads` threads that
/// all start together, and returns the achieved nanoseconds per operation (lower is better).
double runStrategy(const size_t numThreads, const std::function<void()>& work)
{
    std::atomic<size_t> ready{0};
    std::atomic<bool> go{false};
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (size_t t = 0; t < numThreads; ++t)
    {
        threads.emplace_back(
            [&]
            {
                ready.fetch_add(1);
                while (!go.load())
                {
                }
                for (size_t i = 0; i < ITERATIONS_PER_THREAD; ++i)
                {
                    work();
                }
            });
    }
    while (ready.load() < numThreads)
    {
    }
    const auto start = std::chrono::steady_clock::now();
    go.store(true);
    for (auto& thread : threads)
    {
        thread.join();
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    const auto totalOps = static_cast<double>(numThreads * ITERATIONS_PER_THREAD);
    return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()) / totalOps;
}

void report(const std::string& name, const size_t threads, const double nsPerOp)
{
    const double opsPerSec = 1e9 / nsPerOp;
    std::cout << "  " << std::left << std::setw(20) << name << "  threads=" << threads << "  " << std::setw(9) << nsPerOp
              << " ns/op  " << (opsPerSec * threads / 1e6) << " Mops/s (aggregate)\n";
}

void benchmarkSize(const size_t size)
{
    std::cout << "\n=== request size = " << size << " bytes ===\n";
    for (const size_t threads : {size_t{1}, size_t{2}, size_t{4}, size_t{8}})
    {
        {
            auto a1 = BufferManager::create(DEFAULT_SIZE, NUM_BUFFERS, std::make_shared<NesDefaultMemoryAllocator>(), eagerSizeClasses());
            report(
                "A1 SizeClass",
                threads,
                runStrategy(
                    threads,
                    [&]
                    {
                        auto buffer = a1->getBuffer(size);
                        touch(buffer.getAvailableMemoryArea().data(), size);
                    }));
        }
        {
            const size_t blocks = (size + COMPOSE_BLOCK - 1) / COMPOSE_BLOCK;
            auto a2 = BufferManager::create(COMPOSE_BLOCK, NUM_BUFFERS);
            report(
                "A2 ComposeFixed",
                threads,
                runStrategy(
                    threads,
                    [&]
                    {
                        std::vector<TupleBuffer> chain;
                        chain.reserve(blocks);
                        size_t remaining = size;
                        for (size_t b = 0; b < blocks; ++b)
                        {
                            auto buffer = a2->getBufferBlocking();
                            const size_t n = remaining < COMPOSE_BLOCK ? remaining : COMPOSE_BLOCK;
                            touch(buffer.getAvailableMemoryArea().data(), n);
                            remaining -= n;
                            chain.push_back(std::move(buffer));
                        }
                    }));
        }
        {
            VmCache a3(size, NUM_BUFFERS, /*reclaim=*/false);
            report(
                "A3 VmCache-reuse",
                threads,
                runStrategy(
                    threads,
                    [&]
                    {
                        auto* p = a3.alloc();
                        touch(p, size);
                        a3.release(p);
                    }));
        }
        {
            VmCache a3r(size, NUM_BUFFERS, /*reclaim=*/true);
            report(
                "A3 VmCache-reclaim",
                threads,
                runStrategy(
                    threads,
                    [&]
                    {
                        auto* p = a3r.alloc();
                        touch(p, size);
                        a3r.release(p);
                    }));
        }
        {
            auto base = BufferManager::create(DEFAULT_SIZE, NUM_BUFFERS);
            report(
                "Unpooled (legacy)",
                threads,
                runStrategy(
                    threads,
                    [&]
                    {
                        auto buffer = base->getUnpooledBuffer(size);
                        touch(buffer->getAvailableMemoryArea().data(), size);
                    }));
        }
        std::cout << "\n";
    }
}

/// Deterministic internal-fragmentation each scheme carries for a single request (physical bytes it must
/// reserve / bytes the caller asked for). This is the memory dimension of the A1/A2/A3 tradeoff.
void efficiencyTable()
{
    std::cout << "=== memory per allocation (physical reserved / requested) ===\n";
    std::cout << "  scheme               size    reserved   overhead\n";
    for (const size_t size : {SMALL_SIZE, LARGE_SIZE})
    {
        const size_t a1 = nextPow2(size < 512 ? 512 : size);
        const size_t a2 = roundUp(size, COMPOSE_BLOCK);
        const size_t a3 = roundUp(size, PAGE); /// page-granular; reclaim keeps only this resident.
        auto row = [&](const char* name, size_t reserved)
        {
            std::cout << "  " << std::left << std::setw(20) << name << std::right << std::setw(6) << size << std::setw(11)
                      << reserved << "   " << std::fixed << std::setprecision(2)
                      << (static_cast<double>(reserved) / static_cast<double>(size)) << "x\n";
        };
        row("A1 SizeClass", a1);
        row("A2 ComposeFixed", a2);
        row("A3 VmCache", a3);
        std::cout << "\n";
    }
}
}

int main()
{
    std::cout << "BufferManager variable-size allocation microbenchmark (" << ITERATIONS_PER_THREAD
              << " alloc+touch+free per thread)\n";
    benchmarkSize(SMALL_SIZE);
    benchmarkSize(LARGE_SIZE);
    efficiencyTable();
    return 0;
}

/// NOLINTEND(readability-magic-numbers,readability-identifier-length,google-build-using-namespace,cppcoreguidelines-narrowing-conversions,misc-include-cleaner)
