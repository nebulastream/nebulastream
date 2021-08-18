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
#include <array>
#include <benchmark/benchmark.h>
#include <random>
#include <thread>
#include <sys/mman.h>

static constexpr auto RANGE_MIN = 1024 * 1024 * 1024;
static constexpr auto RANGE_MAX = 1024 * 1024 * 1024;
static constexpr auto ITERATIONS = 1;
static constexpr auto REPETITIONS = 3;
static constexpr auto MIN_THREADS = 64;
static constexpr auto MAX_THREADS = 128;
static constexpr auto PAGE_SIZE = 4096;

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
                if (posix_memalign(&src[i], PAGE_SIZE, allocatedAreaSize)) {
                    mlock(src[i], allocatedAreaSize);
                    state.SkipWithError("Cannot allocate mem");
                    return;
                }
                if (posix_memalign(&dst[i], PAGE_SIZE, allocatedAreaSize)) {
                    mlock(dst[i], allocatedAreaSize);
                    state.SkipWithError("Cannot allocate mem");
                    return;
                }
                mlock(dst[i], allocatedAreaSize);
                mlock(src[i], allocatedAreaSize);
                auto* ptr = reinterpret_cast<uint8_t*>(src[i]);
                auto* ptr2 = reinterpret_cast<uint8_t*>(dst[i]);
                for (uint64_t i = 0; i < (allocatedAreaSize / PAGE_SIZE); ++i) {
                    auto* srcWriter = reinterpret_cast<uint64_t*>(ptr);
                    auto* dstWriter = reinterpret_cast<uint64_t*>(ptr2);
                    *srcWriter = distribution(randomness);
                    *dstWriter = distribution(randomness);
                    ptr += PAGE_SIZE;
                    ptr2 += PAGE_SIZE;
                }
            }
        }
    }

    void TearDown(::benchmark::State& state) override {
        size_t chunkSize = state.range(0);
        size_t allocatedAreaSize = state.threads * chunkSize;
        if (state.thread_index == 0) {
            for (auto ptr : src) {
                if (ptr) {
                    munlock(ptr, allocatedAreaSize);
                    free(ptr);
                }
            }

            for (auto ptr : dst) {
                if (ptr) {
                    munlock(ptr, allocatedAreaSize);
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
        ++index;
    }

    if (state.thread_index == 0) {
        state.SetBytesProcessed(int64_t(state.iterations()) * allocatedAreaSize);
    }
}

BENCHMARK_DEFINE_F(MemcpyBenchmark, BM_apex_memcpy)(benchmark::State& state) {
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
        //        }
        ++index;
    }
    if (state.thread_index == 0) {
        state.SetBytesProcessed(int64_t(state.iterations()) * allocatedAreaSize);
    }
}

BENCHMARK_DEFINE_F(MemcpyBenchmark, BM_rte_memcpy)(benchmark::State& state) {
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
        ++index;
    }

    if (state.thread_index == 0) {
        state.SetBytesProcessed(int64_t(state.iterations()) * allocatedAreaSize);
    }
}

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
