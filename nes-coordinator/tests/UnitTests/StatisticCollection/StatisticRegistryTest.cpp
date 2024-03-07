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
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/InfrastructureCharacteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/IngestionRate.hpp>
#include <Operators/LogicalOperators/StatisticCollection/TriggerCondition/NeverTrigger.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticInfo.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <vector>

namespace NES {

class StatisticRegistryTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StatisticRegistryTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StatisticRegistryTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES::Testing::BaseUnitTest::SetUp(); }

    static void TearDownTestCase() {}
};

std::vector<Statistic::StatisticKey> createRandomStatisticKey(const uint64_t numberOfKeys) {
    std::vector<Statistic::StatisticKey> randomKeys;
    for (auto i = 0_u64; i < numberOfKeys; ++i) {
        randomKeys.emplace_back(Statistic::IngestionRate::create(), i);
    }
    return randomKeys;
}

std::vector<Statistic::StatisticInfo> createRandomStatisticInfo(const uint64_t numberOfInfos) {
    std::vector<Statistic::StatisticInfo> randomInfos;
    for (auto i = 0_u64; i < numberOfInfos; ++i) {
        randomInfos.emplace_back(TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(rand())),
                                 Statistic::NeverTrigger::create(),
                                 nullptr,
                                 rand());
    }
    return randomInfos;
}

/**
 * @brief This tests checks, if we can insert and retrieve one statistic
 */
TEST_F(StatisticRegistryTest, singleStatisticTest) {
    {
        Statistic::StatisticRegistry statisticRegistry;
        auto singleStatisticKey = createRandomStatisticKey(1)[0];
        auto singleStatisticInfo = createRandomStatisticInfo(1)[0];
        statisticRegistry.insert(singleStatisticKey, singleStatisticInfo);
        auto returnedStatisticInfo = statisticRegistry.getStatisticInfo(singleStatisticKey);
        auto tmp = (**returnedStatisticInfo);
        ASSERT_EQ(tmp, singleStatisticInfo);
    }
}

/**
 * @brief This tests checks, if we can insert and retrieve multiple statistics
 */
TEST_F(StatisticRegistryTest, multipleStatisticsTest) {
    constexpr auto NUMBER_OF_ENTRIES = 1000;
    auto statisticKeys = createRandomStatisticKey(NUMBER_OF_ENTRIES);
    auto statisticInfos = createRandomStatisticInfo(NUMBER_OF_ENTRIES);
    Statistic::StatisticRegistry statisticRegistry;

    for (auto i = 0_u64; i < NUMBER_OF_ENTRIES; ++i) {
        statisticRegistry.insert(statisticKeys[i], statisticInfos[i]);
    }

    for (auto i = 0_u64; i < NUMBER_OF_ENTRIES; ++i) {
        auto returnedStatisticInfo = statisticRegistry.getStatisticInfo(statisticKeys[i]);
        ASSERT_EQ((**returnedStatisticInfo), statisticInfos[i]);
    }
}

/**
 * @brief This tests checks, if we can insert and retrieve multiple statistics concurrently.
 */
TEST_F(StatisticRegistryTest, complexConcurrentStatisticsTest) {
    constexpr auto NUMBER_OF_ENTRIES = 10000;
    constexpr auto NUMBER_OF_THREADS = 16;

    auto statisticKeys = createRandomStatisticKey(NUMBER_OF_ENTRIES);
    auto statisticInfos = createRandomStatisticInfo(NUMBER_OF_ENTRIES);
    Statistic::StatisticRegistry statisticRegistry;

    std::vector<std::thread> allThreads;
    std::atomic<uint64_t> currentPos = 0;
    for (auto threadId = 0; threadId < NUMBER_OF_THREADS; ++threadId) {
        allThreads.emplace_back([&currentPos, &statisticKeys, &statisticRegistry, &statisticInfos]() {
            auto nextUpdatePos = 0_u64;
            while ((nextUpdatePos = currentPos++) < NUMBER_OF_ENTRIES) {
                statisticRegistry.insert(statisticKeys[nextUpdatePos], statisticInfos[nextUpdatePos]);
            }
        });
    }

    // Waiting till all threads are done
    for (auto& thread : allThreads) {
        thread.join();
    }

    // Comparing output
    for (auto i = 0_u64; i < NUMBER_OF_ENTRIES; ++i) {
        auto returnedStatisticInfo = statisticRegistry.getStatisticInfo(statisticKeys[i]);
        ASSERT_EQ((**returnedStatisticInfo), statisticInfos[i]);
    }
}
}// namespace NES