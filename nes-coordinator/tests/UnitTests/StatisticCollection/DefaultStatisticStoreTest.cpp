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
#include <API/Windowing.hpp>
#include <BaseUnitTest.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Characteristic/InfrastructureCharacteristic.hpp>
#include <Operators/LogicalOperators/StatisticCollection/Statistics/Metrics/IngestionRate.hpp>
#include <StatisticCollection/StatisticStorage/DefaultStatisticStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <numeric>

namespace NES::Statistic {

/**
 * @brief Dummy statistic so that we can test our DefaultStatisticStore
 */
class DummyStatistic : public Statistic {
  public:
    DummyStatistic(Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs)
        : Statistic(startTs, endTs, rand()), randomValue(rand()) {}

    static StatisticPtr create(Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs) {
        return std::make_shared<DummyStatistic>(DummyStatistic(startTs, endTs));
    }

    StatisticValue<> getStatisticValue(const ProbeExpression&) const override { return StatisticValue<>(randomValue); }
    std::string toString() const override {
        return "DummyStatistic (" + startTs.toString() + ", " + endTs.toString() + ", " + std::to_string(observedTuples) + ", "
            + std::to_string(randomValue) + ")";
    }

    bool equal(const Statistic& other) const override {
        if (other.instanceOf<DummyStatistic>()) {
            auto otherDummy = dynamic_cast<const DummyStatistic&>(other);
            return startTs == otherDummy.startTs && endTs == otherDummy.endTs && observedTuples == otherDummy.observedTuples
                && randomValue == otherDummy.randomValue;
        }
        return false;
    }

    ~DummyStatistic() override = default;

    uint64_t randomValue;
};

class DefaultStatisticStoreTest : public Testing::BaseUnitTest, public ::testing::WithParamInterface<std::tuple<int, int, int>> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DefaultStatisticStoreTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DefaultStatisticStoreTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES::Testing::BaseUnitTest::SetUp(); }

    /**
     * @brief Creates statistics for the number of statistics key and returns the largest endTs
     * @param allStatisticsPlusKey
     * @param allStatisticKeys
     * @param numberOfStatisticKeys
     * @param numberOfStatisticPerKey
     * @return
     */
    TimeMeasure createData(std::vector<std::pair<StatisticKey, StatisticPtr>>& allStatisticsPlusKey,
                           std::vector<StatisticKey>& allStatisticKeys,
                           const int numberOfStatisticKeys,
                           const int numberOfStatisticPerKey) {
        using namespace Statistic;
        constexpr auto WINDOW_SIZE = 100;
        auto curStartTs = 0;
        auto curEndTs = WINDOW_SIZE;
        for (auto keyCnt = 0; keyCnt < numberOfStatisticKeys; ++keyCnt) {
            StatisticKey statisticKey(IngestionRate::create(), keyCnt);
            allStatisticKeys.emplace_back(statisticKey);

            // Creating for this statistic key now statistics
            for (auto i = 0; i < numberOfStatisticPerKey; ++i) {
                const auto startTs = Milliseconds(curStartTs);
                const auto endTs = Milliseconds(curEndTs);
                allStatisticsPlusKey.emplace_back(std::make_pair(statisticKey, DummyStatistic::create(startTs, endTs)));
                curStartTs += WINDOW_SIZE;
                curEndTs += WINDOW_SIZE;
            }
        }

        return TimeMeasure(curEndTs);
    }

    DefaultStatisticStore defaultStatisticStore;
};

/**
 * @brief Tests, if we can insert, get and delete one single Statistic
 */
TEST_F(DefaultStatisticStoreTest, singleItem) {
    using namespace Windowing;
    using namespace API;
    const auto startTs = Milliseconds(10);
    const auto endTs = Milliseconds(100);
    auto dummyStatistic = DummyStatistic::create(startTs, endTs);
    auto window = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    StatisticKey statisticKey(IngestionRate::create(), 42);

    // Checking if insert and get works properly
    ASSERT_TRUE(defaultStatisticStore.insertStatistic(statisticKey.hash(), dummyStatistic));
    auto getStatistics = defaultStatisticStore.getStatistics(statisticKey.hash(), startTs, endTs);
    ASSERT_EQ(getStatistics.size(), 1);
    EXPECT_TRUE(getStatistics[0]->equal(*dummyStatistic));

    // Now, we delete the statistic and then check that we can not retrieve it anymore
    ASSERT_TRUE(defaultStatisticStore.deleteStatistics(statisticKey.hash(), startTs, endTs));
    getStatistics = defaultStatisticStore.getStatistics(statisticKey.hash(), startTs, endTs);
    ASSERT_EQ(getStatistics.size(), 0);
}

/**
 * @brief Tests, if we can insert, get and delete multiple items
 */
TEST_P(DefaultStatisticStoreTest, multipleItem) {
    using namespace Windowing;
    using namespace API;

    // Parsing the parameters
    const auto numberOfThreads = std::get<0>(DefaultStatisticStoreTest::GetParam());
    const auto numberOfStatisticKey = std::get<1>(DefaultStatisticStoreTest::GetParam());
    const auto numberOfStatisticsPerKey = std::get<2>(DefaultStatisticStoreTest::GetParam());

    // Creating for each statistic key its statistics
    std::vector<std::pair<StatisticKey, StatisticPtr>> allStatisticsPlusKey;
    std::vector<StatisticKey> allStatisticKeys;
    auto maxEndTs = createData(allStatisticsPlusKey, allStatisticKeys, numberOfStatisticKey, numberOfStatisticsPerKey);

    // Checking if insert and get works properly
    std::vector<std::thread> insertThreads;
    std::atomic<uint64_t> statisticsPos = 0;
    for (auto threadId = 0; threadId < numberOfThreads; ++threadId) {
        insertThreads.emplace_back([&statisticsPos, this, &allStatisticsPlusKey]() {
            uint64_t nextPos;
            while ((nextPos = statisticsPos++) < allStatisticsPlusKey.size()) {
                const auto& statisticKey = allStatisticsPlusKey[nextPos].first;
                const auto& dummyStatistic = allStatisticsPlusKey[nextPos].second;
                ASSERT_TRUE(defaultStatisticStore.insertStatistic(statisticKey.hash(), dummyStatistic));
                auto getStatistics = defaultStatisticStore.getStatistics(statisticKey.hash(),
                                                                         dummyStatistic->getStartTs(),
                                                                         dummyStatistic->getEndTs());
                ASSERT_EQ(getStatistics.size(), 1);
                EXPECT_TRUE(getStatistics[0]->equal(*dummyStatistic));
            }
        });
    }
    for (auto& thread : insertThreads) {
        thread.join();
    }

    // Checking if we can retrieve all inserted statistics
    for (auto& statisticKey : allStatisticKeys) {
        std::vector<StatisticPtr> statistics;
        for (auto& statisticPlusKey : allStatisticsPlusKey) {
            if (statisticPlusKey.first == statisticKey) {
                statistics.emplace_back(statisticPlusKey.second);
            }
        }
        auto getStatistics = defaultStatisticStore.getStatistics(statisticKey.hash(), Milliseconds(0), maxEndTs);
        ASSERT_EQ(getStatistics.size(), statistics.size());
        std::sort(getStatistics.begin(), getStatistics.end(), [](const StatisticPtr& left, const StatisticPtr& right) {
            return left->getStartTs() < right->getStartTs();
        });
        EXPECT_EQ(getStatistics, statistics);
    }

    // Now, we delete the statistic and then check that we can not retrieve it anymore
    std::vector<std::thread> deleteThreads;
    statisticsPos = 0;
    for (auto threadId = 0; threadId < numberOfThreads; ++threadId) {
        deleteThreads.emplace_back([&statisticsPos, this, &allStatisticsPlusKey]() {
            uint64_t nextPos;
            while ((nextPos = statisticsPos++) < allStatisticsPlusKey.size()) {
                const auto& statisticKey = allStatisticsPlusKey[nextPos].first;
                const auto& dummyStatistic = allStatisticsPlusKey[nextPos].second;
                const auto startTs = dummyStatistic->getStartTs();
                const auto endTs = dummyStatistic->getEndTs();
                ASSERT_TRUE(defaultStatisticStore.deleteStatistics(statisticKey.hash(), startTs, endTs));
                auto getStatistics = defaultStatisticStore.getStatistics(statisticKey.hash(), startTs, endTs);
                ASSERT_EQ(getStatistics.size(), 0);
            }
        });
    }
    for (auto& thread : deleteThreads) {
        thread.join();
    }

    // After, we have deleted all items, we should not get any statistic
    for (auto& statisticKey : allStatisticKeys) {
        auto getStatistics = defaultStatisticStore.getStatistics(statisticKey.hash(), Milliseconds(0), maxEndTs);
        ASSERT_EQ(getStatistics.size(), 0);
    }
}

INSTANTIATE_TEST_CASE_P(testDefaultStatisticStore,
                        DefaultStatisticStoreTest,
                        // We test here over threads, number of statistic keys, and number of statistics per key
                        ::testing::Combine(::testing::Values(1, 2, 4, 8),   // No. threads
                                           ::testing::Values(1, 5, 10),     // No. statistic key
                                           ::testing::Values(1, 500, 1000)),// No. statistics per key
                        [](const testing::TestParamInfo<DefaultStatisticStoreTest::ParamType>& info) {
                            return std::string(std::to_string(std::get<0>(info.param)) + "_Threads"
                                               + std::to_string(std::get<1>(info.param)) + "_StatisticKey"
                                               + std::to_string(std::get<2>(info.param)) + "_StatisticsPerKey");
                        });
}// namespace NES::Statistic