
#include <benchmark/benchmark.h>
#include <State/StateManager.hpp>

namespace NES{

/**
 * @brief benchmarks how many updates per second on a single key can be done
 */
static void BM_Statemanager_SingleKey_Updates(benchmark::State& state){
    // Initialize and set up benchmark function
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-0");


    struct Key_Value{
        uint32_t key;
        uint32_t value;
    };
    std::vector<Key_Value> keyValueVec;

    for (size_t i = 0; i < state.range(0); i++) {
        uint32_t key = rand();
        uint32_t val = rand();

        Key_Value kv;
        kv.key = key;
        kv.value = val;
        keyValueVec.push_back(kv);
    }


    for(auto singleState : state){
        for (size_t i = 0; i < state.range(0); i++) {
            Key_Value kv = keyValueVec[i];
            var[kv.key] = kv.value;
        }
    }
}

// Register benchmark
BENCHMARK(BM_Statemanager_SingleKey_Updates)
    ->DenseRange(25, 1000, 25)
    ->Repetitions(10)->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

// A benchmark main is needed
BENCHMARK_MAIN();
};