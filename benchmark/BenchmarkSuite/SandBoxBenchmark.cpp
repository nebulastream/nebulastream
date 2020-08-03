//
// Created by nils on 8/3/20.
//
#include <benchmark/benchmark.h>
#include <cstring>
#include <set>
#include "SandBoxBenchmark.h"

static void BM_StringCreation(benchmark::State& state) {
    char* src = new char[state.range(0)];
    char* dst = new char[state.range(0)];
    memset(src, 'x', state.range(0));
    for (auto _ : state) {
        memcpy(dst, src, state.range(0));
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
    delete[] src;
    delete[] dst;
}

static void BM_SetInsert(benchmark::State& state) {
    std::set<int> data;
    for (auto _ : state) {
        for (int i = 0; i < state.range(0); ++i)
            for (int j = 0; j < state.range(1); ++j)
                data.insert(std::rand());
    }
}



// Register the function as a benchmark
BENCHMARK(BM_SetInsert)->Ranges({{1<<10, 8<<10}, {128, 512}});
BENCHMARK(BM_StringCreation)->RangeMultiplier(2)->Range(8, 1024);

BENCHMARK_MAIN();