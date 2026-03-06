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

#include <cstdint>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

/// Minimal test that exercises BloomFilter metrics on synthetic NLJ-like data.
/// We create two BloomFilters (representing left and right join sides) populated
/// with different key spaces, then replay the exact host-side metrics counting
/// logic used in NLJOperatorHandler::emitSlicesToProbe to produce the NES_INFO output.
class NLJBloomFilterMetricsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NLJBloomFilterMetricsTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup NLJBloomFilterMetricsTest class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(NLJBloomFilterMetricsTest, syntheticMetricsOutput)
{
    /// --- 1. Create BloomFilters mimicking two join sides ---
    /// Left side: 20 tuples with keys 0..19
    /// Right side: 30 tuples with keys 10..39
    /// Overlap: keys 10..19 (10 keys)
    /// Non-overlapping left keys: 0..9 (10 keys, should be misses)
    constexpr uint64_t leftTupleCount = 20;
    constexpr uint64_t rightTupleCount = 30;

    Nautilus::Interface::BloomFilter rightBloomFilter(rightTupleCount, 0.01);
    Nautilus::Interface::BloomFilter leftBloomFilter(leftTupleCount, 0.01);

    // Populate right BloomFilter with keys 10..39
    for (uint64_t i = 10; i < 10 + rightTupleCount; ++i)
    {
        rightBloomFilter.add(i);
    }
    // Populate left BloomFilter with keys 0..19
    for (uint64_t i = 0; i < leftTupleCount; ++i)
    {
        leftBloomFilter.add(i);
    }

    /// --- 2. Replay the exact metrics counting logic from NLJOperatorHandler::emitSlicesToProbe ---
    BloomFilterMetrics bloomFilterMetrics; /// same struct used in NLJOperatorHandler

    uint64_t localChecks = 0;
    uint64_t localMisses = 0;
    const char* outerSide = nullptr;

    // left (20) < right (30), so left is outer
    if (leftTupleCount < rightTupleCount)
    {
        outerSide = "left";
        localChecks = leftTupleCount;
        // Check each left key against right BloomFilter
        for (uint64_t i = 0; i < leftTupleCount; ++i)
        {
            if (!rightBloomFilter.mightContain(i))
            {
                localMisses++;
            }
        }
    }
    else
    {
        outerSide = "right";
        localChecks = rightTupleCount;
        for (uint64_t i = 0; i < rightTupleCount; ++i)
        {
            if (!leftBloomFilter.mightContain(i))
            {
                localMisses++;
            }
        }
    }

    const uint64_t localHits = localChecks - localMisses;

    // Accumulate into thread-safe counters
    bloomFilterMetrics.bloomChecksTotal.fetch_add(localChecks, std::memory_order_relaxed);
    bloomFilterMetrics.bloomMisses.fetch_add(localMisses, std::memory_order_relaxed);
    bloomFilterMetrics.bloomHits.fetch_add(localHits, std::memory_order_relaxed);

    // Simulate false positives: in a real NLJ probe, if bloom says HIT but inner loop finds 0 matches,
    // that's a false positive. For this synthetic test, assume 2 of the hits were false positives.
    constexpr uint64_t simulatedFP = 2;
    bloomFilterMetrics.bloomFalsePositives.fetch_add(simulatedFP, std::memory_order_relaxed);

    /// --- 3. Print the EXACT same NES_INFO lines as NLJOperatorHandler::emitSlicesToProbe ---

    // Per-invocation log
    if (localMisses > 0 && localChecks > 0)
    {
        const double skipRateLocal = (static_cast<double>(localMisses) / localChecks) * 100.0;
        const uint64_t potentialInnerLoopsSaved =
            localMisses * (localChecks == leftTupleCount ? rightTupleCount : leftTupleCount);
        NES_INFO("BloomFilter Analysis: Filtered {} out of {} {} tuples ({:.2f}%) - "
                 "Potentially saved ~{} inner loop iterations",
                 localMisses, localChecks, outerSide, skipRateLocal, potentialInnerLoopsSaved);
    }

    // Cumulative metrics report
    const uint64_t cumChecks = bloomFilterMetrics.bloomChecksTotal.load(std::memory_order_relaxed);
    const uint64_t cumMisses = bloomFilterMetrics.bloomMisses.load(std::memory_order_relaxed);
    const uint64_t cumHits = bloomFilterMetrics.bloomHits.load(std::memory_order_relaxed);
    const uint64_t cumFP = bloomFilterMetrics.bloomFalsePositives.load(std::memory_order_relaxed);
    const double skipRate = cumChecks > 0 ? (static_cast<double>(cumMisses) / cumChecks) * 100.0 : 0.0;
    const double fpRateContext = cumHits > 0 ? (static_cast<double>(cumFP) / cumHits) * 100.0 : 0.0;

    NES_INFO("BloomFilter Metrics [cumulative]: bloom_checks_total={} bloom_misses={} bloom_hits={} "
             "bloom_false_positives={} skip_rate={:.2f}% fp_rate_context={:.2f}%",
             cumChecks, cumMisses, cumHits, cumFP, skipRate, fpRateContext);

    /// --- 4. Basic assertions ---
    // Left keys 0..9 are NOT in right BloomFilter (keys 10..39), so those 10 should be misses.
    // Left keys 10..19 ARE in right BloomFilter, so those 10 are hits.
    // (Bloom filters may have false positives but never false negatives, so misses <= 10)
    EXPECT_GE(localMisses, 0u);
    EXPECT_LE(localMisses, 10u); // at most 10 definite misses (keys 0..9)
    EXPECT_EQ(cumChecks, leftTupleCount); // left is outer (20 < 30)
    EXPECT_EQ(cumChecks, cumMisses + cumHits);
    EXPECT_EQ(cumFP, simulatedFP);

    NES_INFO("NLJBloomFilterMetricsTest::syntheticMetricsOutput completed successfully");
}
}
