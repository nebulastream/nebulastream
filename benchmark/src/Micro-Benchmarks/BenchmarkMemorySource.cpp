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

static void BM_MemorySource_default_memcpy(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        void* src;
        void* dst;
        posix_memalign(&src, 64, state.range(0));
        posix_memalign(&dst, 64, state.range(0));
        auto* ptr = reinterpret_cast<uint64_t*>(src);
        for (uint64_t i = 0; i < state.range(0) / sizeof(uint64_t); ++i) {
            std::random_device randomDevice;
            std::uniform_int_distribution<uint64_t> distribution;
            ptr[i] = distribution(randomDevice);
        }
        state.ResumeTiming();
        memcpy(dst, src, state.range(0));
        free(src);
        free(dst);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}

static void BM_MemorySource_apex_memcpy(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        void* src;
        void* dst;
        posix_memalign(&src, 64, state.range(0));
        posix_memalign(&dst, 64, state.range(0));
        auto* ptr = reinterpret_cast<uint64_t*>(src);
        for (uint64_t i = 0; i < state.range(0) / sizeof(uint64_t); ++i) {
            std::random_device randomDevice;
            std::uniform_int_distribution<uint64_t> distribution;
            ptr[i] = distribution(randomDevice);
        }
        state.ResumeTiming();
        apex_memcpy(dst, src, state.range(0));
        free(src);
        free(dst);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}

static void BM_MemorySource_rte_memcpy(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        void* src;
        void* dst;
        posix_memalign(&src, 64, state.range(0));
        posix_memalign(&dst, 64, state.range(0));
        auto* ptr = reinterpret_cast<uint64_t*>(src);
        for (uint64_t i = 0; i < state.range(0) / sizeof(uint64_t); ++i) {
            std::random_device randomDevice;
            std::uniform_int_distribution<uint64_t> distribution;
            ptr[i] = distribution(randomDevice);
        }
        state.ResumeTiming();
        rte_memcpy(dst, src, state.range(0));
        free(src);
        free(dst);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
}

BENCHMARK(BM_MemorySource_default_memcpy)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(2)
    ->Range(16 * 1024, 8 * 1024 * 1024)
    ->Repetitions(1)
    ->Iterations(1)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_MemorySource_apex_memcpy)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(2)
    ->Range(16 * 1024, 8 * 1024 * 1024)
    ->Repetitions(1)
    ->Iterations(1)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kNanosecond);

BENCHMARK(BM_MemorySource_rte_memcpy)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(2)
    ->Range(16 * 1024, 8 * 1024 * 1024)
    ->Repetitions(1)
    ->Iterations(1)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kNanosecond);