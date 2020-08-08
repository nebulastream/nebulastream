#include <benchmark/benchmark.h>
#include <State/StateManager.hpp>

#include <random>
#include <boost/math/distributions/skew_normal.hpp>

namespace NES{

/**
 * @brief benchmarks how many updates per second on a single key can be done
 */
static void BM_Statemanager_SingleKey_Updates(benchmark::State& state){
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-0");

    int64_t numberOfUpdates = state.range(0);

    uint32_t key = rand();
    for(auto singleState : state){
        for (size_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();
            uint32_t value = rand();
            state.ResumeTiming();

            var[key] = value;
        }
    }

    // We need numberOfUpdates * iterations, as this benchmark will be executed numerous times automatically
    state.SetItemsProcessed(numberOfUpdates * int64_t(state.iterations()));
}

/**
 * @brief benchmarks the number of values per second on a key range drawn from an uniformly distributed
 * @param state.range: 0 = number_of_points, 1 = number_of_keys (can not be greater than INT32_MAX)
 */
static void BM_Statemanager_KeyRange_Uniform_Distribution(benchmark::State& state){
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates = state.range(0);
    int64_t numberOfKeys = state.range(1);

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distrib(0, numberOfKeys-1);

    for(auto singleState : state){
        for (size_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();
            uint32_t key = distrib(generator);
            uint32_t value = rand();
            //std::cout << "Uniform distribution <key, value>: <" << key << ", " << value << ">\n";
            assert(0 <= key && key <= numberOfKeys-1);
            state.ResumeTiming();

            var[key] = value;
        }
    }

    // We need numberOfUpdates * iterations, as this benchmark will be executed numerous times automatically
    state.SetItemsProcessed(numberOfUpdates * int64_t(state.iterations()));
}


/**
 * @brief benchmarks the number of values per second on a key range drawn from a skewed distributed
 * @param state.range: 0 = number_of_points, 1 = number_of_keys
 */
static void BM_Statemanager_KeyRange_Skewed_Distribution(benchmark::State& state){
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates = state.range(0);
    int64_t numberOfKeys = state.range(1);

    std::random_device rd;
    std::default_random_engine noise_generator;

    auto skew_norm_dist = boost::math::skew_normal_distribution<double>(1, 0.25, -15);
    std::uniform_real_distribution<double> uniform_dist(0, 1);

    for(auto singleState : state){
        for (size_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();
            noise_generator.seed(rd());
            auto probability = uniform_dist(noise_generator);

            double singleQuantile = std::max(std::min(boost::math::quantile(skew_norm_dist, probability), 1.0), 0.0);
            uint32_t key = uint32_t(singleQuantile * numberOfKeys);
            uint32_t value = rand();
            state.ResumeTiming();

            var[key] = value;
        }
    }

    // We need numberOfUpdates * iterations, as this benchmark will be executed numerous times automatically
    state.SetItemsProcessed(numberOfUpdates * int64_t(state.iterations()));
}


/**
 * @brief benchmarks the number of values per second
 * @param state.range: 0 = number_of_points, 1 = number_of_keys, 2 = type of distribution (single key, uniform, skewed)
 *                      3 = number_of_threads
 */
static void BM_Multithreaded_Key_Updates(benchmark::State& state){
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates = state.range(0);
    int64_t numberOfKeys = state.range(1);
    int64_t distributionType = state.range(2);

    std::vector<uint32_t> keyVec;

    


    if (distributionType == 0){
        // single key
        uint32_t key = rand();
        for (size_t i = 0; i < numberOfUpdates; ++i) {
            keyVec.push_back(key);
        }
    } else if (distributionType == 1){
        // uniform distribution
        std::random_device random_device;
        std::mt19937 generator(random_device());
        std::uniform_int_distribution<> distrib(0, numberOfKeys-1);

        for (size_t i = 0; i < numberOfUpdates; ++i) {
            uint32_t key = distrib(generator);
            assert(0 <= key && key <= numberOfKeys-1);
            keyVec.push_back(key);
        }
    } else if (distributionType == 2){
        // skewed distribution
        std::random_device rd;
        std::default_random_engine noise_generator;

        auto skew_norm_dist = boost::math::skew_normal_distribution<double>(1, 0.25, -15);
        std::uniform_real_distribution<double> uniform_dist(0, 1);

        for (size_t i = 0; i < numberOfUpdates; ++i){
            noise_generator.seed(rd());
            auto probability = uniform_dist(noise_generator);

            double singleQuantile = std::max(std::min(boost::math::quantile(skew_norm_dist, probability), 1.0), 0.0);
            uint32_t key = uint32_t(singleQuantile * numberOfKeys);
            keyVec.push_back(key);
        }
    }


    for(auto singleState : state){
        for (size_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();
            uint32_t key = keyVec[i];
            uint32_t value = rand();
            state.ResumeTiming();

            var[key] = value;
        }
    }

    // We need numberOfUpdates * iterations, as this benchmark will be executed numerous times automatically
    state.SetItemsProcessed(int64_t(state.range(0)) * int64_t(state.iterations()));
}




/*// Register benchmark
BENCHMARK(BM_Statemanager_SingleKey_Updates)
    ->RangeMultiplier(2)->Range(1e3, 1e7)
    ->Repetitions(3)->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Statemanager_KeyRange_Uniform_Distribution)
    ->RangeMultiplier(2)->Ranges({{1e3, 1e7}, {1e3, 1e6}})
    ->Repetitions(3)->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Statemanager_KeyRange_Skewed_Distribution)
    ->RangeMultiplier(10)->Ranges({{1e3, 1e7}, {1e3, 1e6}})
    ->Repetitions(3)->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);*/


BENCHMARK(BM_Multithreaded_Key_Updates)
    ->RangeMultiplier(10)->Ranges({{1e4, 1e4}, {1e2, 1e2}, {0, 2}, {1, 2}})
    ->Repetitions(3)->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);



// A benchmark main is needed
BENCHMARK_MAIN();
};