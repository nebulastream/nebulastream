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
#include <memory>
#include <random>
#include <vector>

#include <Statistics/DefaultStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <gtest/gtest.h>
#include <Statistic.hpp>

namespace NES
{
namespace
{

Statistic createDummyStatistic(const StatisticId statisticId, const Timestamp startTs, const Timestamp endTs)
{
    static std::mt19937 gen(42);

    /// Generating random statistic data of size 1 to 4 KiB
    std::uniform_int_distribution<size_t> sizeDistribution{1, 4 * 1024};
    const size_t statisticSize = sizeDistribution(gen);
    auto statisticData = std::make_shared<std::byte[]>(statisticSize);
    std::uniform_int_distribution<int> byteDistribution{0, 255};
    for (size_t i = 0; i < statisticSize; ++i)
    {
        statisticData[i] = static_cast<std::byte>(byteDistribution(gen));
    }

    return {statisticId, "ReservoirSample", startTs, endTs, statisticSize, std::move(statisticData), statisticSize};
}

}

TEST(DefaultStatisticStoreTest, insertAndGetSingleStatistic)
{
    DefaultStatisticStore store;
    const StatisticId statisticId{1};
    const auto statistic = createDummyStatistic(statisticId, Timestamp(0), Timestamp(1000));
    ASSERT_TRUE(store.insertStatistic(statisticId, statistic));

    const auto found = store.getSingleStatistic(statisticId, Timestamp(0), Timestamp(1000));
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(*found, statistic);

    /// Only exact window bounds match
    EXPECT_FALSE(store.getSingleStatistic(statisticId, Timestamp(0), Timestamp(999)).has_value());
    EXPECT_FALSE(store.getSingleStatistic(StatisticId(2), Timestamp(0), Timestamp(1000)).has_value());
}

TEST(DefaultStatisticStoreTest, getStatisticsReturnsAllInRange)
{
    DefaultStatisticStore store;
    const StatisticId statisticId{1};
    std::vector<Statistic> inserted;
    for (uint64_t windowStart = 0; windowStart < 5000; windowStart += 1000)
    {
        inserted.push_back(createDummyStatistic(statisticId, Timestamp(windowStart), Timestamp(windowStart + 1000)));
        ASSERT_TRUE(store.insertStatistic(statisticId, inserted.back()));
    }

    EXPECT_EQ(store.getStatistics(statisticId, Timestamp(0), Timestamp(5000)).size(), 5);
    EXPECT_EQ(store.getStatistics(statisticId, Timestamp(1000), Timestamp(3000)).size(), 2);
    EXPECT_TRUE(store.getStatistics(StatisticId(2), Timestamp(0), Timestamp(5000)).empty());
    EXPECT_EQ(store.getAllStatistics().size(), 5);
}

TEST(DefaultStatisticStoreTest, deleteStatisticsRemovesOnlyRange)
{
    DefaultStatisticStore store;
    const StatisticId statisticId{1};
    for (uint64_t windowStart = 0; windowStart < 5000; windowStart += 1000)
    {
        ASSERT_TRUE(
            store.insertStatistic(statisticId, createDummyStatistic(statisticId, Timestamp(windowStart), Timestamp(windowStart + 1000))));
    }

    ASSERT_TRUE(store.deleteStatistics(statisticId, Timestamp(1000), Timestamp(3000)));
    EXPECT_EQ(store.getStatistics(statisticId, Timestamp(0), Timestamp(5000)).size(), 3);
    EXPECT_FALSE(store.deleteStatistics(statisticId, Timestamp(1000), Timestamp(3000)));
    EXPECT_FALSE(store.getSingleStatistic(statisticId, Timestamp(1000), Timestamp(2000)).has_value());
    EXPECT_TRUE(store.getSingleStatistic(statisticId, Timestamp(0), Timestamp(1000)).has_value());
}

}
