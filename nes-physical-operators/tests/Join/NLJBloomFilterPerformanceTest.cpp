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

#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <gtest/gtest.h>
#include <array>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <tuple>

using BloomFilter = NES::Nautilus::Interface::BloomFilter;

namespace {

class NLJBloomFilterPerformanceTestSuite : public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("NLJBloomFilterPerformanceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NLJBloomFilterPerformanceTest class.");
    }

    static void TearDownTestSuite()
    {
        NES_INFO("Tear down NLJBloomFilterPerformanceTest class.");
    }
};

struct Scenario
{
    const char* name;
    uint64_t outerTuples;
    uint64_t innerTuples;
    uint64_t overlapTuples;
};

struct ProbeResult
{
    uint64_t matchCount;
    uint64_t probedOuterTuples;
};

[[nodiscard]] ProbeResult runProbeWithoutBloomFilter(const Scenario& scenario)
{
    const uint64_t firstOverlappingOuterPos = scenario.outerTuples - scenario.overlapTuples;
    uint64_t matchCount = 0;
    for (uint64_t outerPos = 0; outerPos < scenario.outerTuples; ++outerPos)
    {
        for (uint64_t innerPos = 0; innerPos < scenario.innerTuples; ++innerPos)
        {
            if (outerPos >= firstOverlappingOuterPos && innerPos == outerPos - firstOverlappingOuterPos)
            {
                ++matchCount;
            }
        }
    }
    return {.matchCount = matchCount, .probedOuterTuples = scenario.outerTuples};
}

[[nodiscard]] ProbeResult runProbeWithBloomFilter(const Scenario& scenario)
{
    const uint64_t firstOverlappingOuterPos = scenario.outerTuples - scenario.overlapTuples;
    BloomFilter bloomFilter(scenario.overlapTuples, 0.01);
    for (uint64_t innerPos = 0; innerPos < scenario.overlapTuples; ++innerPos)
    {
        bloomFilter.add(firstOverlappingOuterPos + innerPos);
    }

    uint64_t matchCount = 0;
    uint64_t probedOuterTuples = 0;
    for (uint64_t outerPos = 0; outerPos < scenario.outerTuples; ++outerPos)
    {
        if (!bloomFilter.mightContain(outerPos))
        {
            continue;
        }

        ++probedOuterTuples;
        for (uint64_t innerPos = 0; innerPos < scenario.innerTuples; ++innerPos)
        {
            if (outerPos >= firstOverlappingOuterPos && innerPos == outerPos - firstOverlappingOuterPos)
            {
                ++matchCount;
            }
        }
    }
    return {.matchCount = matchCount, .probedOuterTuples = probedOuterTuples};
}

template<typename Fn>
[[nodiscard]] std::chrono::nanoseconds measureNanos(Fn&& fn, const size_t repetitions)
{
    std::chrono::nanoseconds total{0};
    for (size_t i = 0; i < repetitions; ++i)
    {
        const auto start = std::chrono::steady_clock::now();
        fn();
        total += std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - start);
    }
    return total;
}

} // namespace

TEST_F(NLJBloomFilterPerformanceTestSuite, ProbeLoopWorkReductionAndTiming)
{
    constexpr size_t repetitions = 8;
    volatile uint64_t timingSink = 0;
    const std::array<Scenario, 3> scenarios{{
        {.name = "Sparse", .outerTuples = 100'000, .innerTuples = 50'000, .overlapTuples = 1'000},
        {.name = "Medium", .outerTuples = 100'000, .innerTuples = 75'000, .overlapTuples = 10'000},
        {.name = "Dense", .outerTuples = 100'000, .innerTuples = 90'000, .overlapTuples = 50'000},
    }};

    std::cout << "\nNLJ BloomFilter Performance Test\n";
    std::cout << "repetitions=" << repetitions << "\n";

    for (const auto& scenario : scenarios)
    {
        const auto baselineResult = runProbeWithoutBloomFilter(scenario);
        const auto bloomResult = runProbeWithBloomFilter(scenario);

        ASSERT_EQ(baselineResult.matchCount, scenario.overlapTuples);
        ASSERT_EQ(bloomResult.matchCount, scenario.overlapTuples);
        ASSERT_LT(bloomResult.probedOuterTuples, baselineResult.probedOuterTuples);

        const auto baselineTime = measureNanos(
            [&]() { timingSink += runProbeWithoutBloomFilter(scenario).matchCount; }, repetitions);
        const auto bloomTime = measureNanos(
            [&]() { timingSink += runProbeWithBloomFilter(scenario).matchCount; }, repetitions);

        const double averageBaselineMs = static_cast<double>(baselineTime.count()) / 1'000'000.0 / repetitions;
        const double averageBloomMs = static_cast<double>(bloomTime.count()) / 1'000'000.0 / repetitions;
        const double speedup = averageBaselineMs / averageBloomMs;
        const double outerPruning = 100.0
            * static_cast<double>(baselineResult.probedOuterTuples - bloomResult.probedOuterTuples)
            / static_cast<double>(baselineResult.probedOuterTuples);
        const uint64_t baselineComparisons = baselineResult.probedOuterTuples * scenario.innerTuples;
        const uint64_t bloomComparisons = bloomResult.probedOuterTuples * scenario.innerTuples;

        std::cout << "scenario=" << scenario.name
                  << " outer=" << scenario.outerTuples
                  << " inner=" << scenario.innerTuples
                  << " overlap=" << scenario.overlapTuples
                  << " avg_no_bloom_ms=" << averageBaselineMs
                  << " avg_with_bloom_ms=" << averageBloomMs
                  << " speedup=" << speedup << "x"
                  << " comparisons=" << baselineComparisons << "->" << bloomComparisons
                  << " outer_pruning=" << outerPruning << "%\n";

        EXPECT_LT(bloomComparisons, baselineComparisons);
        EXPECT_GT(outerPruning, 0.0);

        if (std::string_view(scenario.name) == "Sparse")
        {
            EXPECT_GT(outerPruning, 90.0);
        }
        else if (std::string_view(scenario.name) == "Medium")
        {
            EXPECT_GT(outerPruning, 70.0);
        }
        else if (std::string_view(scenario.name) == "Dense")
        {
            EXPECT_GT(outerPruning, 30.0);
        }
    }
    EXPECT_GT(timingSink, 0U);
}