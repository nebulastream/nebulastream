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
#include <string>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <StatisticTuple.hpp>

namespace NES
{
namespace
{

/// A statistic whose payload is a single byte carrying 'marker', so tests can tell instances apart.
StatisticTuple makeStatistic(const uint64_t id, const uint64_t startTs, const uint64_t endTs, const std::byte marker = std::byte{0})
{
    /// NOLINTNEXTLINE(modernize-avoid-c-arrays) dynamic byte buffer requires array form
    auto data = std::make_shared<std::byte[]>(1);
    data[0] = marker;
    return StatisticTuple{
        StatisticId{id},
        StatisticBlobType{AvgAggregationLogicalFunction::getName()},
        Timestamp{startTs},
        Timestamp{endTs},
        /*numberOfSeenMeasurements=*/1,
        std::move(data),
        /*statisticDataSize=*/1};
}

}

class StatisticStoreTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticStoreTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        globalStatisticStore().clear();
    }

    void TearDown() override
    {
        globalStatisticStore().clear();
        BaseUnitTest::TearDown();
    }
};

TEST_F(StatisticStoreTest, InsertedStatisticIsReturnedByRangeQuery)
{
    DefaultStatisticStore store;
    const auto statistic = makeStatistic(1, 0, 10);
    ASSERT_TRUE(store.insertStatistic(StatisticId{1}, statistic));

    const auto found = store.getStatistics(StatisticId{1}, Timestamp{0}, Timestamp{10});
    ASSERT_EQ(found.size(), 1U);
    EXPECT_EQ(*found.front(), statistic);
}

TEST_F(StatisticStoreTest, RangeQueryIsInclusiveAndExcludesWindowsOutsideIt)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 0, 10));
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 10, 20));
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 20, 30));

    /// [0, 20] fully contains the first two windows but only clips the third.
    EXPECT_EQ(store.getStatistics(StatisticId{1}, Timestamp{0}, Timestamp{20}).size(), 2U);
    EXPECT_EQ(store.getStatistics(StatisticId{1}, Timestamp{0}, Timestamp{30}).size(), 3U);
}

TEST_F(StatisticStoreTest, UnknownStatisticIdYieldsNothing)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 0, 10));

    EXPECT_TRUE(store.getStatistics(StatisticId{2}, Timestamp{0}, Timestamp{10}).empty());
    EXPECT_FALSE(store.getSingleStatistic(StatisticId{2}, Timestamp{0}, Timestamp{10}).has_value());
}

TEST_F(StatisticStoreTest, SingleStatisticRequiresAnExactWindowMatch)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 10, 20));

    EXPECT_TRUE(store.getSingleStatistic(StatisticId{1}, Timestamp{10}, Timestamp{20}).has_value());
    /// A range that merely contains the window is not an exact match.
    EXPECT_FALSE(store.getSingleStatistic(StatisticId{1}, Timestamp{0}, Timestamp{30}).has_value());
}

TEST_F(StatisticStoreTest, DeleteRemovesOnlyFullyContainedWindows)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 0, 10));
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 20, 30));

    ASSERT_TRUE(store.deleteStatistics(StatisticId{1}, Timestamp{0}, Timestamp{10}));
    const auto remaining = store.getStatistics(StatisticId{1}, Timestamp{0}, Timestamp{30});
    ASSERT_EQ(remaining.size(), 1U);
    EXPECT_EQ(remaining.front()->getStartTs().getRawValue(), 20U);

    /// Nothing left in that range, so the second delete reports no work done.
    EXPECT_FALSE(store.deleteStatistics(StatisticId{1}, Timestamp{0}, Timestamp{10}));
}

TEST_F(StatisticStoreTest, StoreDoesNotDeduplicate)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 0, 10, std::byte{1}));
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 0, 10, std::byte{2}));

    EXPECT_EQ(store.getStatistics(StatisticId{1}, Timestamp{0}, Timestamp{10}).size(), 2U);
}

TEST_F(StatisticStoreTest, GetAllStatisticsSpansEveryId)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticId{1}, makeStatistic(1, 0, 10));
    store.insertStatistic(StatisticId{2}, makeStatistic(2, 0, 10));
    store.insertStatistic(StatisticId{2}, makeStatistic(2, 10, 20));

    EXPECT_EQ(store.getAllStatistics().size(), 3U);
}

TEST_F(StatisticStoreTest, GlobalStoreIsShared)
{
    globalStatisticStore().insertStatistic(StatisticId{7}, makeStatistic(7, 0, 10));
    EXPECT_EQ(globalStatisticStore().getStatistics(StatisticId{7}, Timestamp{0}, Timestamp{10}).size(), 1U);
}

TEST_F(StatisticStoreTest, GlobalStoreClearDropsEverything)
{
    globalStatisticStore().insertStatistic(StatisticId{1}, makeStatistic(1, 0, 10));
    globalStatisticStore().clear();
    EXPECT_TRUE(globalStatisticStore().getStatistics(StatisticId{1}, Timestamp{0}, Timestamp{10}).empty());
}

}
