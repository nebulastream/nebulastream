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
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <chrono>
#include <random>
#include <vector>
#include <algorithm>
#include <cmath>

namespace NES
{

/**
 * @brief Test BloomFilter performance with different join selectivity rates
 * Selectivity shows how many outer tuples can actually match inner tuples.
 * - Sparse 1%: Few matching tuples : BloomFilter is helpful
 * - Medium 10%: Some matches : BloomFilter still helpful
 * - Dense 50%: Many matches : BloomFilter less helpful
 */
class BloomFilterSelectivityTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("BloomFilterSelectivityTest.log", LogLevel::LOG_INFO);
    }

    static void TearDownTestCase() { }
};

TEST_F(BloomFilterSelectivityTest, SelectivityImpactWithRandomIDs)
{
    // Test: Messe BloomFilter-Speedup bei verschiedenen Join-Selectivity Raten
    // Nutze RANDOM IDs
    // Kleinere Datenmengen für schnelle Test-Ausführung / kann erhöht werden

    constexpr uint64_t innerTupleCount = 50000;   
    constexpr uint64_t outerTupleCount = 50000;   
    constexpr double fpRate = 0.01; 
    constexpr size_t repetitions = 1;

    const double selectivities[] = {0.01, 0.10, 0.50};
    const char* selectivityNames[] = {"Sparse (1%)", "Medium (10%)", "Dense (50%)"};

    NES_INFO("BloomFilter Selectivity Impact Test");
    NES_INFO("Inner tuples: {}", innerTupleCount);
    NES_INFO("Outer tuples: {}", outerTupleCount);
    NES_INFO("FP Rate: {}%", fpRate * 100.0);
    NES_INFO("Testing different join selectivity rates with RANDOM IDs");
    NES_INFO("");

    for (size_t s = 0; s < 3; ++s)
    {
        double selectivity = selectivities[s];
        const char* name = selectivityNames[s];

        NES_INFO(" Scenario: {} ", name);

        // Generate random IDs for inner side
        std::mt19937 rng(42);
        std::uniform_int_distribution<uint64_t> idDist(0, UINT64_MAX);

        std::vector<uint64_t> innerIDs;
        innerIDs.reserve(innerTupleCount);
        for (uint64_t i = 0; i < innerTupleCount; ++i)
        {
            innerIDs.push_back(idDist(rng));
        }

        // Generate outer IDs: some match inner (selectivity %), rest are random misses
        std::vector<uint64_t> outerIDs;
        outerIDs.reserve(outerTupleCount);
        uint64_t matchingOuterTuples = static_cast<uint64_t>(outerTupleCount * selectivity);

        for (uint64_t i = 0; i < outerTupleCount; ++i)
        {

            if (i < matchingOuterTuples)
            {
                // This outer tuple should match (random from inner)
                uint64_t innerIdx = idDist(rng) % innerTupleCount;
                outerIDs.push_back(innerIDs[innerIdx]);
            }
            else

            {
                // This outer tuple should NOT match (random ID)
                outerIDs.push_back(idDist(rng));
            }
        }

        // Shuffle outer IDs to mix matching and non-matching
        std::shuffle(outerIDs.begin(), outerIDs.end(), rng);

        // WITHOUT BloomFilter
        auto baselineTest = [&]() {
            uint64_t joinResults = 0;
            for (uint64_t outerID : outerIDs)
            {
                for (uint64_t innerID : innerIDs)
                {
                    if (outerID == innerID)
                    {
                        joinResults++;
                    }
                }
            }
            return joinResults;
        };

        // WITH BloomFilter
        struct ProbeStats
        {
            uint64_t joinResults;
            uint64_t candidateOuterTuples;
        };

        auto bloomFilterTest = [&]() {
            Nautilus::Interface::BloomFilter bloomFilter(innerTupleCount, fpRate);

            // Build phase: add all inner IDs to BloomFilter
            for (uint64_t innerID : innerIDs)
            {
                bloomFilter.add(innerID);
            }

            // Probe phase: filter outer IDs
            uint64_t joinResults = 0;
            uint64_t candidateOuterTuples = 0;

            for (uint64_t outerID : outerIDs)
            {
                // Early filtering: if BloomFilter says "no", skip inner loop entirely
                if (!bloomFilter.mightContain(outerID))
                {
                    continue;
                }

                ++candidateOuterTuples;

                // Only check inner tuples for possibly matching outer IDs
                for (uint64_t innerID : innerIDs)
                {
                    if (outerID == innerID)
                    {
                        joinResults++;
                        break;
                    }
                }
            }

            return ProbeStats{joinResults, candidateOuterTuples};
        };

        // Verify correctness
        uint64_t baselineResult = baselineTest();
        ProbeStats bloomResult = bloomFilterTest();
        ASSERT_EQ(baselineResult, bloomResult.joinResults)
            << "Join results should be identical with and without BloomFilter";

        // Measure performance
        auto baselineTotalNanos = std::chrono::nanoseconds(0);
        auto bloomTotalNanos = std::chrono::nanoseconds(0);

        volatile uint64_t timingGuard = 0;
        for (size_t rep = 0; rep < repetitions; ++rep)
        {
            auto start = std::chrono::high_resolution_clock::now();
            timingGuard += baselineTest();
            auto end = std::chrono::high_resolution_clock::now();
            baselineTotalNanos += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);

            start = std::chrono::high_resolution_clock::now();
            timingGuard += bloomFilterTest().joinResults;
            end = std::chrono::high_resolution_clock::now();
            bloomTotalNanos += std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        }

        ASSERT_GT(timingGuard, 0U);

        double baselineAvgMs = static_cast<double>(baselineTotalNanos.count()) / 1'000'000.0 / repetitions;
        double bloomAvgMs = static_cast<double>(bloomTotalNanos.count()) / 1'000'000.0 / repetitions;
        double speedup = baselineAvgMs / bloomAvgMs;
        const uint64_t baselineComparisons = outerTupleCount * innerTupleCount;
        const uint64_t bloomComparisons = bloomResult.candidateOuterTuples * innerTupleCount;
        const double outerPruning = 100.0 * static_cast<double>(outerTupleCount - bloomResult.candidateOuterTuples)
            / static_cast<double>(outerTupleCount);

        NES_INFO("Baseline (NO BloomFilter): {:.2f} ms", baselineAvgMs);
        NES_INFO("WITH BloomFilter: {:.2f} ms", bloomAvgMs);
        NES_INFO("Speedup: {:.2f}x", speedup);
        NES_INFO("Candidate outer tuples: {} / {}", bloomResult.candidateOuterTuples, outerTupleCount);
        NES_INFO("Comparisons: {} -> {}", baselineComparisons, bloomComparisons);
        NES_INFO("Outer pruning: {:.2f}%", outerPruning);
        NES_INFO("Join matches: {}", baselineResult);
        NES_INFO("Expected matches: ~{}", matchingOuterTuples);
        NES_INFO("");

        // Assertions based on deterministic work reduction (robust across machines)
        ASSERT_LT(bloomComparisons, baselineComparisons)
            << "BloomFilter should reduce probe-loop comparisons";
        ASSERT_LT(bloomResult.candidateOuterTuples, outerTupleCount)
            << "BloomFilter should prune at least some outer tuples";

        // Different selectivity scenarios should have different pruning behavior
        if (selectivity == 0.01)
        {
            ASSERT_GT(outerPruning, 90.0) << "Sparse joins should prune most outer tuples (>90%)";
        }
        else if (selectivity == 0.10)
        {
            ASSERT_GT(outerPruning, 70.0) << "Medium joins should prune strongly (>70%)";
        }
        else if (selectivity == 0.50)
        {
            ASSERT_GT(outerPruning, 30.0) << "Dense joins should still prune a meaningful share (>30%)";
        }
    }

    NES_INFO("Summary");
    NES_INFO("BloomFilter effectiveness scales with selectivity");
    NES_INFO("");
}

} // namespace NES
