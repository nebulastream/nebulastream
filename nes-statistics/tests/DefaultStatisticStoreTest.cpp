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
#include <ranges>
#include <../include/StatisticStore/DefaultStatisticStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>
#include <Statistic.hpp>

namespace NES
{
namespace
{

Statistic createDummyStatistic(const Statistic::StatisticId statisticId, Windowing::TimeMeasure startTs, Windowing::TimeMeasure endTs)
{
    std::random_device rd;
    std::mt19937 gen(rd());

    /// Picking a random statistic type value
    constexpr auto statisticTypes = magic_enum::enum_values<Statistic::StatisticType>();
    std::uniform_int_distribution<> enumDistribution{0, magic_enum::enum_count<Statistic::StatisticType>()};
    const auto randomStatisticType = statisticTypes[enumDistribution(gen)];

    /// Generating random statistic data of size 1 to 100 KiB
    std::uniform_int_distribution<> sizeDistribution{1, 100 * 1024};
    const size_t statisticSize = sizeDistribution(gen);
    std::vector<int8_t> statisticData(statisticSize);
    std::uniform_int_distribution<> byteDistribution{0, 255};
    for (size_t i = 0; i < statisticSize; ++i)
    {
        statisticData[i] = byteDistribution(gen);
    }

    return {statisticId, randomStatisticType, startTs, endTs, statisticSize, statisticData.data(), statisticSize};
}

}

class DefaultStatisticStoreTest : public Testing::BaseUnitTest, public testing::WithParamInterface<std::tuple<int, int, int, int>>
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("DefaultStatisticStoreTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup DefaultStatisticStoreTest test class.");
    }

    /// Will be called before a test is executed.
    void SetUp() override { BaseUnitTest::SetUp(); }

    static std::pair<Windowing::TimeMeasure, std::vector<Statistic>>
    createData(const int numberOfStatisticIds, const int numberOfStatisticPerId, const uint64_t windowSize)
    {
        std::vector<Statistic> statistics;
        uint64_t maxEndTs = 0;

        for (auto statisticId = 0; statisticId < numberOfStatisticIds; ++statisticId)
        {
            uint64_t curStartTs = 0;
            uint64_t curEndTs = windowSize;

            /// Creating for this statistic id now statistics
            for (auto i = 0; i < numberOfStatisticPerId; ++i)
            {
                const Windowing::TimeMeasure startTs{curStartTs};
                const Windowing::TimeMeasure endTs{curEndTs};

                statistics.emplace_back(createDummyStatistic(statisticId, startTs, endTs));
                curStartTs += windowSize;
                curEndTs += windowSize;
                maxEndTs = std::max(maxEndTs, curEndTs);
            }
        }

        return {Windowing::TimeMeasure{maxEndTs}, statistics};
    }

    DefaultStatisticStore defaultStatisticStore;
};

TEST_F(DefaultStatisticStoreTest, singleItem)
{
    constexpr auto numberOfStatisticIds = 1;
    constexpr auto numberOfStatisticPerId = 1;
    constexpr auto windowSize = 10;
    auto [maxEndTimestamp, statistics] = createData(numberOfStatisticIds, numberOfStatisticPerId, windowSize);
    const auto& dummyStatistic = statistics[0];

    /// Checking if insert and get works properly
    ASSERT_TRUE(defaultStatisticStore.insertStatistic(dummyStatistic.getHash(), dummyStatistic));
    auto getStatistics
        = defaultStatisticStore.getStatistics(dummyStatistic.getHash(), dummyStatistic.getStartTs(), dummyStatistic.getEndTs());
    ASSERT_EQ(getStatistics.size(), 1);
    EXPECT_TRUE(*getStatistics[0] == dummyStatistic);

    /// Now, we delete the statistic and then check that we can not retrieve it anymore
    ASSERT_TRUE(defaultStatisticStore.deleteStatistics(dummyStatistic.getHash(), dummyStatistic.getStartTs(), dummyStatistic.getEndTs()));
    getStatistics = defaultStatisticStore.getStatistics(dummyStatistic.getHash(), dummyStatistic.getStartTs(), dummyStatistic.getEndTs());
    ASSERT_EQ(getStatistics.size(), 0);
}

TEST_P(DefaultStatisticStoreTest, multipleItem)
{
    /// Parsing the parameters
    const auto numberOfThreads = std::get<0>(GetParam());
    const auto numberOfStatisticIds = std::get<1>(GetParam());
    const auto numberOfStatisticPerId = std::get<2>(GetParam());
    const auto windowSize = std::get<3>(GetParam());

    /// Creating for each statistic key its statistics
    auto [maxEndTs, allStatistics] = createData(numberOfStatisticIds, numberOfStatisticPerId, windowSize);

    /// Checking if insert and get works properly
    std::vector<std::thread> insertThreads;
    std::atomic<uint64_t> statisticsPos = 0;
    for (auto threadId = 0; threadId < numberOfThreads; ++threadId)
    {
        insertThreads.emplace_back(
            [&statisticsPos, this, &allStatistics]()
            {
                uint64_t nextPos;
                while ((nextPos = statisticsPos++) < allStatistics.size())
                {
                    const auto& dummyStatistic = allStatistics[nextPos];
                    ASSERT_TRUE(defaultStatisticStore.insertStatistic(dummyStatistic.getHash(), dummyStatistic));
                    auto getStatistics = defaultStatisticStore.getStatistics(
                        dummyStatistic.getHash(), dummyStatistic.getStartTs(), dummyStatistic.getEndTs());
                    ASSERT_EQ(getStatistics.size(), 1);
                    EXPECT_TRUE(*getStatistics[0] == dummyStatistic);
                }
            });
    }
    for (auto& thread : insertThreads)
    {
        thread.join();
    }


    /// Checking, if we can retrieve all inserted statistics via getAllStatistics() and that we can find every retrieved actual statistics
    /// in the created dummy statistics
    const auto allActualStatistics = defaultStatisticStore.getAllStatistics();
    ASSERT_EQ(allActualStatistics.size(), allStatistics.size());
    for (const auto& actualStatistic : allActualStatistics | std::views::values)
    {
        const auto foundStatistic
            = std::ranges::any_of(allStatistics, [&actualStatistic](const auto& statistic) { return *actualStatistic == statistic; });
        EXPECT_TRUE(foundStatistic) << fmt::format("Could not find {} in the statistics store!", *actualStatistic);
    }

    /// Checking, if we can retrieve all inserted statistics for a particular statistic hash
    for (auto& dummyStatistic : allStatistics)
    {
        auto retrievedStatistics = defaultStatisticStore.getStatistics(dummyStatistic.getHash(), Windowing::TimeMeasure{0}, maxEndTs);

        const auto foundStatistic
            = std::ranges::any_of(retrievedStatistics, [&dummyStatistic](const auto& statistic) { return dummyStatistic == *statistic; });
        EXPECT_TRUE(foundStatistic) << fmt::format("Could not find {} in the statistics store!", dummyStatistic);
    }

    /// Now, we delete the statistic and then check that we can not retrieve it anymore
    std::vector<std::thread> deleteThreads;
    statisticsPos = 0;
    for (auto threadId = 0; threadId < numberOfThreads; ++threadId)
    {
        deleteThreads.emplace_back(
            [&statisticsPos, this, &allStatistics]()
            {
                uint64_t nextPos;
                while ((nextPos = statisticsPos++) < allStatistics.size())
                {
                    const auto& dummyStatistic = allStatistics[nextPos];
                    const auto startTs = dummyStatistic.getStartTs();
                    const auto endTs = dummyStatistic.getEndTs();
                    ASSERT_TRUE(defaultStatisticStore.deleteStatistics(dummyStatistic.getHash(), startTs, endTs));
                    auto getStatistics = defaultStatisticStore.getStatistics(dummyStatistic.getHash(), startTs, endTs);
                    ASSERT_EQ(getStatistics.size(), 0);
                }
            });
    }
    for (auto& thread : deleteThreads)
    {
        thread.join();
    }

    /// After, we have deleted all items, we should not get any statistic
    ASSERT_EQ(defaultStatisticStore.getAllStatistics().size(), 0);
    for (auto& dummyStatistic : allStatistics)
    {
        auto getStatistics = defaultStatisticStore.getStatistics(dummyStatistic.getHash(), Windowing::TimeMeasure{0}, maxEndTs);
        ASSERT_EQ(getStatistics.size(), 0);
    }
}

INSTANTIATE_TEST_CASE_P(
    testDefaultStatisticStore,
    DefaultStatisticStoreTest,
    ::testing::Combine(
        ::testing::Values(1, 2, 4, 8), /// No. threads
        ::testing::Values(1, 5, 10), /// No. statistic ids
        ::testing::Values(1, 500, 1000), /// No. statistics per ids
        ::testing::Values(1, 10, 1000)), /// Window Size
    [](const testing::TestParamInfo<DefaultStatisticStoreTest::ParamType>& info)
    {
        return std::string(
            std::to_string(std::get<0>(info.param)) + "_Threads" + std::to_string(std::get<1>(info.param)) + "_StatisticId"
            + std::to_string(std::get<2>(info.param)) + "_StatisticsPerId" + std::to_string(std::get<3>(info.param)) + "_WindowSize");
    });
}
