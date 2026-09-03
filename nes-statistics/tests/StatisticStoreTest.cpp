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
#include <StatisticTuple.hpp>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <StatisticStore/StatisticStoreRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{

/// A statistic whose payload is a single byte carrying 'marker', so tests can tell instances apart.
StatisticTuple makeStatistic(const uint64_t id, const uint64_t startTs, const uint64_t endTs, const std::byte marker = std::byte{0})
{
    auto data = std::make_shared<std::byte[]>(1);
    data[0] = marker;
    return StatisticTuple{
        StatisticTuple::StatisticId{id},
        StatisticTuple::StatisticType::Avg,
        Windowing::TimeMeasure{startTs},
        Windowing::TimeMeasure{endTs},
        /*numberOfSeenTuples=*/1,
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
        StatisticStoreRegistry::instance().clear();
    }

    void TearDown() override
    {
        StatisticStoreRegistry::instance().clear();
        BaseUnitTest::TearDown();
    }
};

TEST_F(StatisticStoreTest, InsertedStatisticIsReturnedByRangeQuery)
{
    DefaultStatisticStore store;
    const auto statistic = makeStatistic(1, 0, 10);
    ASSERT_TRUE(store.insertStatistic(StatisticTuple::StatisticId{1}, statistic));

    const auto found = store.getStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10});
    ASSERT_EQ(found.size(), 1U);
    EXPECT_EQ(found.front(), statistic);
}

TEST_F(StatisticStoreTest, RangeQueryIsInclusiveAndExcludesWindowsOutsideIt)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 0, 10));
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 10, 20));
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 20, 30));

    /// [0, 20] fully contains the first two windows but only clips the third.
    EXPECT_EQ(store.getStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{20}).size(), 2U);
    EXPECT_EQ(store.getStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{30}).size(), 3U);
}

TEST_F(StatisticStoreTest, UnknownStatisticIdYieldsNothing)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 0, 10));

    EXPECT_TRUE(store.getStatistics(StatisticTuple::StatisticId{2}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10}).empty());
    EXPECT_FALSE(store.getSingleStatistic(StatisticTuple::StatisticId{2}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10}).has_value());
}

TEST_F(StatisticStoreTest, SingleStatisticRequiresAnExactWindowMatch)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 10, 20));

    EXPECT_TRUE(store.getSingleStatistic(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{10}, Windowing::TimeMeasure{20}).has_value());
    /// A range that merely contains the window is not an exact match.
    EXPECT_FALSE(store.getSingleStatistic(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{30}).has_value());
}

TEST_F(StatisticStoreTest, DeleteRemovesOnlyFullyContainedWindows)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 0, 10));
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 20, 30));

    ASSERT_TRUE(store.deleteStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10}));
    const auto remaining = store.getStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{30});
    ASSERT_EQ(remaining.size(), 1U);
    EXPECT_EQ(remaining.front().getStartTs().getTime(), 20U);

    /// Nothing left in that range, so the second delete reports no work done.
    EXPECT_FALSE(store.deleteStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10}));
}

TEST_F(StatisticStoreTest, StoreDoesNotDeduplicate)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 0, 10, std::byte{1}));
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 0, 10, std::byte{2}));

    EXPECT_EQ(store.getStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10}).size(), 2U);
}

TEST_F(StatisticStoreTest, GetAllStatisticsSpansEveryId)
{
    DefaultStatisticStore store;
    store.insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 0, 10));
    store.insertStatistic(StatisticTuple::StatisticId{2}, makeStatistic(2, 0, 10));
    store.insertStatistic(StatisticTuple::StatisticId{2}, makeStatistic(2, 10, 20));

    EXPECT_EQ(store.getAllStatistics().size(), 3U);
}

/// The registry is what lets the writer's handler, the reader's handler and the statistic interface reach one store
/// without threading it through NodeEngine and every lowering rule, so identity by name is the contract.
TEST_F(StatisticStoreTest, RegistryReturnsTheSameStoreForTheSameName)
{
    auto& registry = StatisticStoreRegistry::instance();
    const auto first = registry.getOrCreate(std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME});
    const auto second = registry.getOrCreate(std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME});

    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first, second);

    /// A write through one handle is visible through the other.
    first->insertStatistic(StatisticTuple::StatisticId{7}, makeStatistic(7, 0, 10));
    EXPECT_EQ(second->getStatistics(StatisticTuple::StatisticId{7}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10}).size(), 1U);
}

TEST_F(StatisticStoreTest, RegistryKeepsDistinctNamesApart)
{
    auto& registry = StatisticStoreRegistry::instance();
    const auto storeA = registry.getOrCreate("a");
    const auto storeB = registry.getOrCreate("b");

    EXPECT_NE(storeA, storeB);

    storeA->insertStatistic(StatisticTuple::StatisticId{1}, makeStatistic(1, 0, 10));
    EXPECT_TRUE(storeB->getStatistics(StatisticTuple::StatisticId{1}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{10}).empty());
}

TEST_F(StatisticStoreTest, RegistryClearDropsPreviousStores)
{
    auto& registry = StatisticStoreRegistry::instance();
    const auto before = registry.getOrCreate("a");
    registry.clear();
    const auto after = registry.getOrCreate("a");

    EXPECT_NE(before, after);
}

}
