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

#include <random>
#include <variant>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <StatisticStore/SubStoresStatisticStore.hpp>
#include <StatisticStore/WindowStatisticStore.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <benchmark/benchmark.h>
#include <magic_enum/magic_enum.hpp>
#include <Statistic.hpp>

namespace NES
{
namespace
{

/// Configurable min/max statistic data size in bytes
constexpr int MIN_STATISTIC_DATA_SIZE = 1;
constexpr int MAX_STATISTIC_DATA_SIZE = 100 * 1024;

/// Output CSV filename
constexpr std::string_view BENCHMARK_CSV = "statistic_store_benchmark.csv";

Statistic createDummyStatistic(const Statistic::StatisticId statisticId, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs)
{
    std::random_device rd;
    std::mt19937 gen(rd());

    /// Picking a random statistic type value
    constexpr auto statisticTypes = magic_enum::enum_values<Statistic::StatisticType>();
    std::uniform_int_distribution<> enumDistribution{0, magic_enum::enum_count<Statistic::StatisticType>()};
    const auto randomStatisticType = statisticTypes[enumDistribution(gen)];

    /// Generating random statistic data
    std::uniform_int_distribution<> sizeDistribution{MIN_STATISTIC_DATA_SIZE, MAX_STATISTIC_DATA_SIZE};
    const size_t statisticSize = sizeDistribution(gen);
    std::vector<int8_t> statisticData(statisticSize);
    std::uniform_int_distribution<> byteDistribution{0, 255};
    for (size_t i = 0; i < statisticSize; ++i)
    {
        statisticData[i] = byteDistribution(gen);
    }

    return {statisticId, randomStatisticType, startTs, endTs, statisticSize, statisticData.data(), statisticSize};
}

using StatisticStoreVariant = std::variant<DefaultStatisticStore, WindowStatisticStore, SubStoresStatisticStore>;

/// Creates a statistic store variant based on the StatisticStoreType enum
StatisticStoreVariant createStore(StatisticStoreType storeType, int numThreads, uint64_t windowSize)
{
    switch (storeType)
    {
        case StatisticStoreType::DEFAULT:
            return DefaultStatisticStore{};
        case StatisticStoreType::WINDOW:
            return WindowStatisticStore{static_cast<uint64_t>(numThreads), Windowing::TimeMeasure{windowSize}};
        case StatisticStoreType::SUB_STORES:
            return SubStoresStatisticStore{static_cast<uint64_t>(numThreads)};
    }
    return DefaultStatisticStore{};
}

/// Benchmark insertStatistic throughput
/// Args: {storeType, windowSize, numberOfStatisticIds}
static void BM_InsertStatistic(benchmark::State& state)
{
    const auto storeType = static_cast<StatisticStoreType>(state.range(0));
    const auto windowSize = static_cast<uint64_t>(state.range(1));
    const auto numberOfStatisticIds = static_cast<int>(state.range(2));

    auto store = createStore(storeType, 1, windowSize);

    uint64_t curTs = 0;
    for (auto _ : state)
    {
        const Statistic::StatisticId statisticId = curTs % numberOfStatisticIds;
        const Windowing::TimeMeasure startTs{curTs};
        const Windowing::TimeMeasure endTs{curTs + windowSize};
        auto statistic = createDummyStatistic(statisticId, startTs, endTs);
        const auto hash = statistic.getHash();

        std::visit([&](auto& s) { s.insertStatistic(hash, std::move(statistic)); }, store);
        curTs += windowSize;
    }

    state.SetItemsProcessed(state.iterations());
    state.SetLabel(std::string{magic_enum::enum_name(storeType)});
}

/// Benchmark getStatistics latency
/// Args: {storeType, windowSize, numberOfStatisticIds}
static void BM_GetStatistics(benchmark::State& state)
{
    const auto storeType = static_cast<StatisticStoreType>(state.range(0));
    const auto windowSize = static_cast<uint64_t>(state.range(1));
    const auto numberOfStatisticIds = static_cast<int>(state.range(2));
    constexpr int numStatisticsPerKey = 100;

    auto store = createStore(storeType, 1, windowSize);

    /// Pre-populate the store
    for (int id = 0; id < numberOfStatisticIds; ++id)
    {
        uint64_t curTs = 0;
        for (int i = 0; i < numStatisticsPerKey; ++i)
        {
            const Windowing::TimeMeasure startTs{curTs};
            const Windowing::TimeMeasure endTs{curTs + windowSize};
            auto statistic = createDummyStatistic(id, startTs, endTs);
            const auto hash = statistic.getHash();
            std::visit([&](auto& s) { s.insertStatistic(hash, std::move(statistic)); }, store);
            curTs += windowSize;
        }
    }

    /// Benchmark retrieving statistics
    std::mt19937 gen{42};
    std::uniform_int_distribution<> idDist{0, numberOfStatisticIds - 1};
    const uint64_t maxTs = numStatisticsPerKey * windowSize;

    for (auto _ : state)
    {
        const Statistic::StatisticId statisticId = idDist(gen);
        /// Create a dummy statistic just to get its hash (hash depends on statistic type)
        auto dummyStatistic = createDummyStatistic(statisticId, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{windowSize});
        const auto hash = dummyStatistic.getHash();

        auto result = std::visit(
            [&](auto& s) { return s.getStatistics(hash, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{maxTs}); }, store);
        benchmark::DoNotOptimize(result);
    }

    state.SetLabel(std::string{magic_enum::enum_name(storeType)});
}

/// Helper to get the integer value for a StatisticStoreType for use in benchmark Args
constexpr auto DEFAULT = static_cast<int>(StatisticStoreType::DEFAULT);
constexpr auto WINDOW = static_cast<int>(StatisticStoreType::WINDOW);
constexpr auto SUB_STORES = static_cast<int>(StatisticStoreType::SUB_STORES);

/// Register benchmarks for all three store types with varying parameters
/// Args: {storeType, windowSize, numberOfStatisticIds}
/// Run with: --benchmark_out=insert_statistic_benchmark.csv --benchmark_out_format=csv
BENCHMARK(BM_InsertStatistic)
    ->Args({DEFAULT, 10, 1})
    ->Args({DEFAULT, 10, 10})
    ->Args({DEFAULT, 100, 1})
    ->Args({DEFAULT, 100, 10})
    ->Args({DEFAULT, 1000, 1})
    ->Args({DEFAULT, 1000, 10})
    ->Args({WINDOW, 10, 1})
    ->Args({WINDOW, 10, 10})
    ->Args({WINDOW, 100, 1})
    ->Args({WINDOW, 100, 10})
    ->Args({WINDOW, 1000, 1})
    ->Args({WINDOW, 1000, 10})
    ->Args({SUB_STORES, 10, 1})
    ->Args({SUB_STORES, 10, 10})
    ->Args({SUB_STORES, 100, 1})
    ->Args({SUB_STORES, 100, 10})
    ->Args({SUB_STORES, 1000, 1})
    ->Args({SUB_STORES, 1000, 10});

/// Run with: --benchmark_out=get_statistics_benchmark.csv --benchmark_out_format=csv
BENCHMARK(BM_GetStatistics)
    ->Args({DEFAULT, 10, 1})
    ->Args({DEFAULT, 10, 10})
    ->Args({DEFAULT, 100, 1})
    ->Args({DEFAULT, 100, 10})
    ->Args({DEFAULT, 1000, 1})
    ->Args({DEFAULT, 1000, 10})
    ->Args({WINDOW, 10, 1})
    ->Args({WINDOW, 10, 10})
    ->Args({WINDOW, 100, 1})
    ->Args({WINDOW, 100, 10})
    ->Args({WINDOW, 1000, 1})
    ->Args({WINDOW, 1000, 10})
    ->Args({SUB_STORES, 10, 1})
    ->Args({SUB_STORES, 10, 10})
    ->Args({SUB_STORES, 100, 1})
    ->Args({SUB_STORES, 100, 10})
    ->Args({SUB_STORES, 1000, 1})
    ->Args({SUB_STORES, 1000, 10});

}
}

int main(int argc, char** argv)
{
    std::vector<char*> args(argv, argv + argc);
    auto outArg = std::string{"--benchmark_out="}.append(NES::BENCHMARK_CSV);
    std::string fmtArg = "--benchmark_out_format=csv";
    args.push_back(outArg.data());
    args.push_back(fmtArg.data());
    argc = static_cast<int>(args.size());
    argv = args.data();

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
