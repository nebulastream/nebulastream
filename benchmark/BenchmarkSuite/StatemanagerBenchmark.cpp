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

#include <State/StateManager.hpp>

#include <benchmark/benchmark.h>
#include <boost/math/distributions/skew_normal.hpp>
#include <random>

namespace NES {

/**
 * @brief benchmarks how many updates per second on a single key can be done
 */
static void BM_Statemanager_SingleKey_Updates(benchmark::State& state) {
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-0");

    int64_t numberOfUpdates = state.range(0);

    uint32_t key = rand();
    for (auto singleState : state) {
        for (int64_t i = 0; i < numberOfUpdates; ++i) {
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
 * @brief benchmarks how many updates per second on a single key can be done, a key vector is used
 */
static void BM_Statemanager_SingleKey_Updates_KeyVec(benchmark::State& state) {
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-0");

    int64_t numberOfUpdates = state.range(0);

    std::vector<uint32_t> keyVec;
    uint32_t key = rand();
    for (int64_t i = 0; i < numberOfUpdates; ++i)
        keyVec.push_back(key);

    for (auto singleState : state) {
        for (int64_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();
            uint32_t tmpKey = keyVec[i];
            uint32_t value = rand();
            state.ResumeTiming();

            var[tmpKey] = value;
        }
    }

    // We need numberOfUpdates * iterations, as this benchmark will be executed numerous times automatically
    state.SetItemsProcessed(numberOfUpdates * int64_t(state.iterations()));
}

/**
 * @brief benchmarks the number of values per second on a key range drawn from an uniformly distributed
 * @param state.range: 0 = number_of_points, 1 = number_of_keys (can not be greater than INT32_MAX)
 */
static void BM_Statemanager_KeyRange_Uniform_Distribution(benchmark::State& state) {
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates = state.range(0);
    int64_t numberOfKeys = state.range(1);

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distrib(0, numberOfKeys - 1);

    for (auto singleState : state) {
        for (int64_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();
            uint32_t key = distrib(generator);
            uint32_t value = rand();
            assert(0 <= key && key <= numberOfKeys - 1);
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
static void BM_Statemanager_KeyRange_Uniform_Distribution_KeyVec(benchmark::State& state) {
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates = state.range(0);
    int64_t numberOfKeys = state.range(1);

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distrib(0, numberOfKeys - 1);

    std::vector<uint32_t> keyVec;
    for (int64_t i = 0; i < numberOfUpdates; ++i) {
        uint32_t key = distrib(generator);
        assert(0 <= key && key <= numberOfKeys - 1);
        keyVec.push_back(key);
    }

    for (auto singleState : state) {
        for (int64_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();
            uint32_t key = keyVec[i];
            uint32_t value = rand();
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
static void BM_Statemanager_KeyRange_Skewed_Distribution(benchmark::State& state) {
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates = state.range(0);
    int64_t numberOfKeys = state.range(1);

    std::random_device rd;
    std::default_random_engine noise_generator;

    auto skew_norm_dist = boost::math::skew_normal_distribution<double>(1, 0.25, -15);
    std::uniform_real_distribution<double> uniform_dist(0, 1);

    for (auto singleState : state) {
        for (int64_t i = 0; i < numberOfUpdates; ++i) {
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
 * @brief benchmarks the number of values per second on a key range drawn from a skewed distributed
 * @param state.range: 0 = number_of_points, 1 = number_of_keys
 */
static void BM_Statemanager_KeyRange_Skewed_Distribution_KeyVec(benchmark::State& state) {
    StateManager& stateManager = StateManager::instance();
    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates = state.range(0);
    int64_t numberOfKeys = state.range(1);

    std::random_device rd;
    std::default_random_engine noise_generator;

    auto skew_norm_dist = boost::math::skew_normal_distribution<double>(1, 0.25, -15);
    std::uniform_real_distribution<double> uniform_dist(0, 1);

    std::vector<uint32_t> keyVec;

    for (int64_t i = 0; i < numberOfUpdates; ++i) {
        noise_generator.seed(rd());
        auto probability = uniform_dist(noise_generator);

        double singleQuantile = std::max(std::min(boost::math::quantile(skew_norm_dist, probability), 1.0), 0.0);
        uint32_t key = uint32_t(singleQuantile * numberOfKeys);
        keyVec.push_back(key);
    }

    for (auto singleState : state) {
        for (int64_t i = 0; i < numberOfUpdates; ++i) {
            state.PauseTiming();

            uint32_t key = keyVec[i];
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
static void BM_Multithreaded_Key_Updates(benchmark::State& state) {

    StateManager& stateManager = StateManager::instance();

    StateVariable<uint32_t, uint32_t>& var = stateManager.registerState<uint32_t, uint32_t>("window-content-1");

    int64_t numberOfUpdates, numberOfKeys, distributionType, numThreads;
    numberOfUpdates = state.range(0);
    numberOfKeys = state.range(1);
    distributionType = state.range(2);
    numThreads = state.range(3);

    std::vector<uint32_t> keyVec;

    if (distributionType == 0) {
        // single key
        uint32_t key = rand();
        for (int64_t i = 0; i < numberOfUpdates; ++i)
            keyVec.push_back(key);

    } else if (distributionType == 1) {
        // uniform distribution
        std::random_device random_device;
        std::mt19937 generator(random_device());
        std::uniform_int_distribution<> distrib(0, numberOfKeys - 1);

        for (int64_t i = 0; i < numberOfUpdates; ++i) {
            uint32_t key = distrib(generator);
            assert(0 <= key && key <= numberOfKeys - 1);
            keyVec.push_back(key);
        }
    } else if (distributionType == 2) {
        // skewed distribution
        std::random_device rd;
        std::default_random_engine noise_generator;

        auto skew_norm_dist = boost::math::skew_normal_distribution<double>(1, 0.25, -15);
        std::uniform_real_distribution<double> uniform_dist(0, 1);

        for (int64_t i = 0; i < numberOfUpdates; ++i) {
            noise_generator.seed(rd());
            auto probability = uniform_dist(noise_generator);

            double singleQuantile = std::max(std::min(boost::math::quantile(skew_norm_dist, probability), 1.0), 0.0);
            uint32_t key = uint32_t(singleQuantile * numberOfKeys);
            keyVec.push_back(key);
        }
    }

    std::vector<std::thread> t;

    for (auto singleState : state) {
        for (uint32_t thread_id = 0; thread_id < numThreads; ++thread_id) {
            t.emplace_back([&var, &state, &numberOfUpdates, &keyVec]() {
                state.PauseTiming();
                shuffle(keyVec.begin(), keyVec.end(), std::default_random_engine());
                for (int64_t i = 0; i < numberOfUpdates; ++i) {
                    state.PauseTiming();
                    uint32_t key = keyVec[i];
                    uint32_t value = rand();
                    state.ResumeTiming();

                    var[key] = value;
                }
                state.PauseTiming();
            });
        }
    }

    for (auto& worker : t) {
        worker.join();
    }

    // We need numberOfUpdates * iterations * numThreads, as this benchmark will be executed numerous times automatically
    state.SetItemsProcessed(int64_t(state.range(0)) * int64_t(state.iterations()) * numThreads);
}

// Register benchmark
BENCHMARK(BM_Statemanager_SingleKey_Updates)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(10)
    ->Range(1e4, 1e7)
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Statemanager_SingleKey_Updates_KeyVec)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(10)
    ->Range(1e4, 1e7)
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Statemanager_KeyRange_Uniform_Distribution)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}})
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Statemanager_KeyRange_Uniform_Distribution_KeyVec)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}})
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Statemanager_KeyRange_Skewed_Distribution)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}})
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Statemanager_KeyRange_Skewed_Distribution_KeyVec)
    //Number of Updates, Number of Keys
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}})
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Multithreaded_Key_Updates)
    //Number of Updates, Number of Keys, Distribution Type, Number of Threads
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {1, 2}})
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {3, 4}})
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {5, 6}})
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {7, 8}})
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {9, 10}})
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {11, 12}})
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {13, 14}})
    ->RangeMultiplier(1000)
    ->Ranges({{1e7, 1e7}, {1, 1e7}, {0, 2}, {15, 16}})
    ->Repetitions(5)
    ->ReportAggregatesOnly(true)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv))
        return 1;
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}

};// namespace NES