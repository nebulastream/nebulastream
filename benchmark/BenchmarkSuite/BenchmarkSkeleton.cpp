
#include <benchmark/benchmark.h>

static void BM_SkeletonFunction(benchmark::State& state){
    // Initialize and set up benchmark function

    for(auto singleState : state){
        // Everything here gets timed
    }
}

// Register benchmark
BENCHMARK(BM_SkeletonFunction);

// A benchmark main is needed
BENCHMARK_MAIN();