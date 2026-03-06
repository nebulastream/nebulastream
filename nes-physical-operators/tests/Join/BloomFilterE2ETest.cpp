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

#include <gtest/gtest.h>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <chrono>
#include <iostream>
#include <random>

namespace NES
{

class BloomFilterE2ETest : public testing::Test
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("BloomFilterE2ETest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup BloomFilterE2ETest class.");
    }

    static void TearDownTestSuite()
    {
        NES_INFO("Tear down BloomFilterE2ETest class.");
    }


protected:
    void SetUp() override
    {
    }

    void TearDown() override
    {
    }
};

/**
 * CORRECTNESS TEST: verify BloomFilter filtering preserves join correctness.
 */
TEST_F(BloomFilterE2ETest, CorrectnessValidation)
{
    const uint64_t totalItems = 1000;
    const uint64_t matchingItems = 50;

    std::cout << "\nCorrectness Validation " << std::endl;

    // Build BloomFilter with items 0-49
    constexpr double falsePositiveRate = 0.01;
    Nautilus::Interface::BloomFilter filter(matchingItems, falsePositiveRate);

    for (uint64_t i = 0; i < matchingItems; ++i)
    {
        filter.add(i);
    }

    // Get filtered positions
    std::vector<uint64_t> filtered;
    for (uint64_t i = 0; i < totalItems; ++i)
    {
        if (filter.mightContain(i))
        {
            filtered.push_back(i);
        }
    }

    // Verify all matching items are included (no false negatives)
    for (uint64_t i = 0; i < matchingItems; ++i)
    {
        EXPECT_TRUE(std::find(filtered.begin(), filtered.end(), i) != filtered.end())
            << "Matching item " << i << " should be in filtered set (no false negatives allowed)";
    }

    // Count false positives
    uint64_t falsePositives = 0;
    for (uint64_t pos : filtered)
    {
        if (pos >= matchingItems)
        {
            falsePositives++;
        }
    }

    const double measuredFPR = static_cast<double>(falsePositives) / (totalItems - matchingItems);
    
    std::cout << "Matching items: " << matchingItems << std::endl;
    std::cout << "Filtered set size: " << filtered.size() << std::endl;
    std::cout << "False positives: " << falsePositives << std::endl;
    std::cout << "Measured FPR: " << (measuredFPR * 100) << "%" << std::endl;

    EXPECT_LE(measuredFPR, 0.03) 
        << "False positive rate should be close to configured 1% (allowing 3% due to randomness)";
    EXPECT_EQ(filtered.size() - matchingItems, falsePositives)
        << "Filtered set should contain exactly matching items + false positives";

    std::cout << "Correctness verified: No false negatives, acceptable false positive rate" << std::endl;
}

} // namespace NES
