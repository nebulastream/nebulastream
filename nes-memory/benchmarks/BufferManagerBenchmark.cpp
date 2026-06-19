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
/// manifest) comparing the three allocation paths of the buffer manager. It is the performance gate
/// for the size-class work: the size-class pooled path must be on par with the legacy fixed pooled
/// path and clearly faster than the unpooled path, and must scale across threads via the lock-free
/// per-class queues.
///
/// Each iteration allocates one buffer and immediately releases it (RAII), so a pool of N buffers is
/// never exhausted by up to N concurrent threads.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace
{
using namespace NES;

constexpr uint32_t DEFAULT_SIZE = 8192;
constexpr uint32_t NUM_BUFFERS = 16384;
/// Maps to a dedicated power-of-two size class (not the default class) and is a valid unpooled size.
constexpr size_t VARIABLE_SIZE = 1024;
constexpr size_t ITERATIONS_PER_THREAD = 2'000'000;

SizeClassConfig eagerSizeClasses()
{
    SizeClassConfig config{.minClassSize = 512, .maxClassSize = 65536};
    config.policy = BufferProvisioningPolicy::EagerPerClass;
    config.buffersPerClass = NUM_BUFFERS;
    return config;
}

/// Runs `work` (one alloc+free) ITERATIONS_PER_THREAD times on each of `numThreads` threads that all
/// start together, and returns the achieved nanoseconds per operation (lower is better).
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
                    /// spin until all threads are ready
                }
                for (size_t i = 0; i < ITERATIONS_PER_THREAD; ++i)
                {
                    work();
                }
            });
    }
    while (ready.load() < numThreads)
    {
        /// wait for all threads to be spun up
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
    std::cout << "  " << name << "  threads=" << threads << "  " << nsPerOp << " ns/op  " << (opsPerSec * threads / 1e6)
              << " Mops/s (aggregate)\n";
}
}

int main()
{
    std::cout << "BufferManager allocation microbenchmark (" << ITERATIONS_PER_THREAD << " alloc+free per thread)\n";

    for (const size_t threads : {size_t{1}, size_t{2}, size_t{4}, size_t{8}})
    {
        auto fixed = BufferManager::create(DEFAULT_SIZE, NUM_BUFFERS);
        report(
            "FixedPooled    ",
            threads,
            runStrategy(
                threads,
                [&]
                {
                    auto buffer = fixed->getBufferBlocking();
                    (void)buffer;
                }));
        fixed.reset();

        auto sizeClass
            = BufferManager::create(DEFAULT_SIZE, NUM_BUFFERS, std::make_shared<NesDefaultMemoryAllocator>(), 64, eagerSizeClasses());
        report(
            "SizeClassPooled",
            threads,
            runStrategy(
                threads,
                [&]
                {
                    auto buffer = sizeClass->getBuffer(VARIABLE_SIZE);
                    (void)buffer;
                }));
        sizeClass.reset();

        auto unpooled = BufferManager::create(DEFAULT_SIZE, NUM_BUFFERS);
        report(
            "Unpooled       ",
            threads,
            runStrategy(
                threads,
                [&]
                {
                    auto buffer = unpooled->getUnpooledBuffer(VARIABLE_SIZE);
                    (void)buffer;
                }));
        unpooled.reset();
        std::cout << "\n";
    }
    return 0;
}
