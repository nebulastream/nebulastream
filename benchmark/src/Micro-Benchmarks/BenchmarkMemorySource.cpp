/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Runtime/internal/apex_memmove.hpp>
#include <Runtime/internal/rte_memory.h>
#include <benchmark/benchmark.h>
#include <random>
#include <thread>
#include <array>

static constexpr auto RANGE_MIN = 16 * 1024 * 1024;       // 16kb
static constexpr auto RANGE_MAX = 256 * 1024 * 1024;// 32MB
static constexpr auto ITERATIONS = 1;
static constexpr auto REPETITIONS = 10;
static constexpr auto MIN_THREADS = 8;
static constexpr auto MAX_THREADS = 32;

class MemcpyBenchmark : public benchmark::Fixture {
  public:
    void SetUp(::benchmark::State& state) override {
        std::mt19937_64 randomness;
        std::uniform_int_distribution<uint64_t> distribution;
        size_t chunkSize = state.range(0);
        size_t allocatedAreaSize = state.threads * chunkSize;
        if (state.thread_index == 0) {
            src.fill(nullptr);
            dst.fill(nullptr);
            for (int i = 0; i < REPETITIONS; ++i) {
                if (posix_memalign(&src[i], 4096, allocatedAreaSize)) {
                    state.SkipWithError("Cannot allocate");
                    return;
                }
                if (posix_memalign(&dst[i], 4096, allocatedAreaSize)) {
                    state.SkipWithError("Cannot allocate");
                    return;
                }
                auto* ptr = reinterpret_cast<uint8_t*>(src[i]);
                auto* ptr2 = reinterpret_cast<uint8_t*>(dst[i]);
//                auto* ptr = reinterpret_cast<uint64_t*>(src[i]);
//                auto* ptr2 = reinterpret_cast<uint64_t*>(dst[i]);
                for (uint64_t i = 0; i < (allocatedAreaSize / 4096); ++i) {
                    auto* a = reinterpret_cast<uint64_t*>(ptr);
                    auto* b = reinterpret_cast<uint64_t*>(ptr2);
                    *a = distribution(randomness);
                    *b = distribution(randomness);
                    ptr += 4096;
                    ptr2 += 4096;
                }
//                for (uint64_t i = 0; i < allocatedAreaSize / sizeof(uint64_t); ++i) {
//                    ptr[i] = ptr2[(allocatedAreaSize / sizeof(uint64_t)) - i] = distribution(randomness);
//                }
            }

        }
    }

    void TearDown(::benchmark::State& state) override {
        if (state.thread_index == 0) {
            for (auto ptr : src) {
                if (ptr) {
                    free(ptr);
                }
            }

            for (auto ptr : dst) {
                if (ptr) {
                    free(ptr);
                }
            }
        }
    }

  protected:
    std::array<void*, REPETITIONS> src = {nullptr};
    std::array<void*, REPETITIONS> dst = {nullptr};
};

BENCHMARK_DEFINE_F(MemcpyBenchmark, BM_default_memcpy)(benchmark::State& state) {

    std::random_device randomDevice;
    std::uniform_int_distribution<uint64_t> distribution;
    size_t chunkSize = state.range(0);
    size_t allocatedAreaSize = state.threads * chunkSize;

    int index = 0;
    for (auto iter : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto* startSrc = reinterpret_cast<uint8_t*>(src[index]) + (state.thread_index * chunkSize);
        auto* startDst = reinterpret_cast<uint8_t*>(dst[index]) + (state.thread_index * chunkSize);
        memcpy(startDst, startSrc, chunkSize);
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        // scramble again
//        auto* it = reinterpret_cast<uint64_t*>(startSrc);
//        auto* endIt = reinterpret_cast<uint64_t*>(startSrc + chunkSize);
//        for (; it != endIt; ++it) {
//            *it = distribution(randomDevice);
//        }
        ++index;
    }

    if (state.thread_index == 0) {
        state.SetBytesProcessed(int64_t(state.iterations()) * allocatedAreaSize);
    }
}

BENCHMARK_DEFINE_F(MemcpyBenchmark, BM_apex_memcpy)(benchmark::State& state) {
    std::random_device randomDevice;
    std::uniform_int_distribution<uint64_t> distribution;
    size_t chunkSize = state.range(0);
    size_t allocatedAreaSize = state.threads * chunkSize;

    int index = 0;
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto* startSrc = reinterpret_cast<uint8_t*>(src[index]) + (state.thread_index * chunkSize);
        auto* startDst = reinterpret_cast<uint8_t*>(dst[index]) + (state.thread_index * chunkSize);
        apex_memcpy(startDst, startSrc, chunkSize);
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        // scramble again
//        auto* it = reinterpret_cast<uint64_t*>(startSrc);
//        auto* endIt = reinterpret_cast<uint64_t*>(startSrc + chunkSize);
//        for (; it != endIt; ++it) {
//            *it = distribution(randomDevice);
//        }
        ++index;
    }
    if (state.thread_index == 0) {
        state.SetBytesProcessed(int64_t(state.iterations()) * allocatedAreaSize);
    }
}

BENCHMARK_DEFINE_F(MemcpyBenchmark, BM_rte_memcpy)(benchmark::State& state) {
    std::random_device randomDevice;
    std::uniform_int_distribution<uint64_t> distribution;
    size_t chunkSize = state.range(0);
    size_t allocatedAreaSize = state.threads * chunkSize;

    int index = 0;
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto* startSrc = reinterpret_cast<uint8_t*>(src[index]) + (state.thread_index * chunkSize);
        auto* startDst = reinterpret_cast<uint8_t*>(dst[index]) + (state.thread_index * chunkSize);
        rte_memcpy(startDst, startSrc, chunkSize);
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double>>(end - start);

        state.SetIterationTime(elapsed_seconds.count());
        // scramble again
//        auto* it = reinterpret_cast<uint64_t*>(startSrc);
//        auto* endIt = reinterpret_cast<uint64_t*>(startSrc + chunkSize);
//        for (; it != endIt; ++it) {
//            *it = distribution(randomDevice);
//        }
        ++index;
    }

    if (state.thread_index == 0) {
        state.SetBytesProcessed(int64_t(state.iterations()) * allocatedAreaSize);
    }
}

BENCHMARK_REGISTER_F(MemcpyBenchmark, BM_default_memcpy)
    ->RangeMultiplier(2)
    ->Range(RANGE_MIN, RANGE_MAX)
    ->Repetitions(REPETITIONS)
    ->Iterations(ITERATIONS)
    ->ReportAggregatesOnly(true)
    ->ThreadRange(MIN_THREADS, MAX_THREADS)
    ->UseManualTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(MemcpyBenchmark, BM_rte_memcpy)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(2)
    ->Range(RANGE_MIN, RANGE_MAX)
    ->Repetitions(REPETITIONS)
    ->Iterations(ITERATIONS)
    ->ReportAggregatesOnly(true)
    ->ThreadRange(MIN_THREADS, MAX_THREADS)
    ->UseManualTime()
    ->Unit(benchmark::kNanosecond);

BENCHMARK_REGISTER_F(MemcpyBenchmark, BM_apex_memcpy)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(2)
    ->Range(RANGE_MIN, RANGE_MAX)
    ->Repetitions(REPETITIONS)
    ->Iterations(ITERATIONS)
    ->ReportAggregatesOnly(true)
    ->ThreadRange(MIN_THREADS, MAX_THREADS)
    ->UseManualTime()
    ->Unit(benchmark::kNanosecond);
