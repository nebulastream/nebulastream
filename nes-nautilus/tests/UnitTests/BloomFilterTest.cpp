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

#include <cstddef>
#include <cstdint>
#include <Nautilus/Interface/Hash/BloomFilter.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES::Nautilus::Interface
{

class BloomFilterTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("BloomFilterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup BloomFilterTest class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down BloomFilterTest class."); }
};

TEST_F(BloomFilterTest, noFalseNegatives)
{
    constexpr std::size_t numKeys = 1000;
    constexpr double fpRate = 0.01;

    BloomFilter filter(numKeys, fpRate);

    for (std::uint64_t key = 0; key < numKeys; ++key)
    {
        filter.add(key);
    }

    for (std::uint64_t key = 0; key < numKeys; ++key)
    {
        ASSERT_TRUE(filter.mightContain(key)) << "False negative for key " << key;
    }
}

TEST_F(BloomFilterTest, clearResetsFilter)
{
    constexpr std::size_t numKeys = 100;
    constexpr double fpRate = 0.01;

    BloomFilter filter(numKeys, fpRate);

    for (std::uint64_t key = 0; key < numKeys; ++key)
    {
        filter.add(key);
    }

    for (std::uint64_t key = 0; key < numKeys; ++key)
    {
        ASSERT_TRUE(filter.mightContain(key)) << "Key " << key << " should be present before clear";
    }

    filter.clear();

    for (std::uint64_t key = 0; key < numKeys; ++key)
    {
        ASSERT_FALSE(filter.mightContain(key)) << "Key " << key << " should not be present after clear";
    }
}

TEST_F(BloomFilterTest, falsePositiveRateSanity)
{
    constexpr std::size_t numInserted = 1000;
    constexpr std::size_t numQueries = 10000;
    constexpr double configuredFpRate = 0.01;
    constexpr double maxAcceptableFpRate = 0.05;

    BloomFilter filter(numInserted, configuredFpRate);

    for (std::uint64_t key = 0; key < numInserted; ++key)
    {
        filter.add(key);
    }

    std::size_t falsePositives = 0;
    constexpr std::uint64_t queryOffset = 1000000;

    for (std::uint64_t i = 0; i < numQueries; ++i)
    {
        std::uint64_t key = queryOffset + i;
        if (filter.mightContain(key))
        {
            ++falsePositives;
        }
    }

    const double actualFpRate = static_cast<double>(falsePositives) / static_cast<double>(numQueries);

    ASSERT_LE(actualFpRate, maxAcceptableFpRate)
        << "False positive rate " << actualFpRate << " exceeds threshold " << maxAcceptableFpRate;

    NES_INFO("False positive rate: {} (configured: {})", actualFpRate, configuredFpRate);
}

TEST_F(BloomFilterTest, edgeCaseZeroExpectedEntries)
{
    BloomFilter filter(0, 0.01);

    ASSERT_GT(filter.sizeInBits(), 0) << "Filter should have non-zero size even with 0 expected entries";
    ASSERT_GT(filter.hashFunctionCount(), 0) << "Filter should have at least one hash function";

    filter.add(42);
    ASSERT_TRUE(filter.mightContain(42));
}

TEST_F(BloomFilterTest, edgeCaseInvalidFalsePositiveRate)
{
    BloomFilter filter1(100, -0.5);
    ASSERT_GT(filter1.sizeInBits(), 0) << "Filter should handle negative FP rate";

    BloomFilter filter2(100, 0.0);
    ASSERT_GT(filter2.sizeInBits(), 0) << "Filter should handle zero FP rate";

    BloomFilter filter3(100, 1.0);
    ASSERT_GT(filter3.sizeInBits(), 0) << "Filter should handle FP rate = 1.0";

    BloomFilter filter4(100, 1.5);
    ASSERT_GT(filter4.sizeInBits(), 0) << "Filter should handle FP rate > 1.0";
}

TEST_F(BloomFilterTest, sameKeyMultipleInserts)
{
    constexpr std::size_t numKeys = 100;
    constexpr double fpRate = 0.01;

    BloomFilter filter(numKeys, fpRate);

    constexpr std::uint64_t key = 42;

    filter.add(key);
    filter.add(key);
    filter.add(key);

    ASSERT_TRUE(filter.mightContain(key)) << "Key should still be present after multiple inserts";
}

TEST_F(BloomFilterTest, gettersReturnReasonableValues)
{
    constexpr std::size_t numKeys = 1000;
    constexpr double fpRate = 0.01;

    BloomFilter filter(numKeys, fpRate);

    ASSERT_GT(filter.sizeInBits(), 0) << "Bit count should be positive";
    ASSERT_GT(filter.hashFunctionCount(), 0) << "Hash function count should be positive";

    NES_INFO("For {} entries with FP rate {}: m={} bits, k={} hashes",
             numKeys, fpRate, filter.sizeInBits(), filter.hashFunctionCount());
}

} // namespace NES::Nautilus::Interface
