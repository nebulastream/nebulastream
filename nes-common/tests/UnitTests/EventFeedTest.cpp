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

#include <Feeds/EventFeed.hpp>

#include <chrono>
#include <optional>
#include <string_view>
#include <tuple>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
constexpr std::chrono::milliseconds POP_TIMEOUT{100};
const Host WORKER_A{"localhost:8080"};
const Host WORKER_B{"localhost:8081"};
const Host UNKNOWN_WORKER{"localhost:9999"};
constexpr std::string_view FEED = "test_feed";
}

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
class EventFeedTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("EventFeedTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup EventFeedTest test class.");
    }

    void TearDown() override
    {
        EventFeedRegistry::instance().clear();
        BaseUnitTest::TearDown();
    }
};

TEST_F(EventFeedTest, DropsRowsWhenFull)
{
    EventFeed feed(2);
    EXPECT_EQ(feed.capacity(), 2);

    EXPECT_TRUE(feed.tryPush("first"));
    EXPECT_TRUE(feed.tryPush("second"));
    EXPECT_FALSE(feed.tryPush("third")) << "A full feed must drop rather than block its producer";
    EXPECT_EQ(feed.droppedRows(), 1);

    const auto first = feed.tryPop(POP_TIMEOUT);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first.value(), "first");

    const auto second = feed.tryPop(POP_TIMEOUT);
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second.value(), "second");

    EXPECT_FALSE(feed.tryPop(POP_TIMEOUT).has_value()) << "An empty feed must time out rather than return a row";
}

TEST_F(EventFeedTest, AcquiringAFeedThatWasNeverCreatedFails)
{
    EXPECT_THROW(EventFeedRegistry::instance().acquire(WORKER_A, FEED), Exception)
        << "A consumer must not be able to conjure up a feed, that would turn a typo into an empty stream";
}

TEST_F(EventFeedTest, CreatingTheSameFeedTwiceOnOneWorkerFails)
{
    const auto producer = EventFeedRegistry::instance().create(WORKER_A, FEED, 8);
    EXPECT_THROW(EventFeedRegistry::instance().create(WORKER_A, FEED, 8), Exception);
}

TEST_F(EventFeedTest, OnlyOneConsumerMayReadAFeed)
{
    auto producer = EventFeedRegistry::instance().create(WORKER_A, FEED, 8);
    EXPECT_FALSE(producer->hasConsumer());

    {
        const auto consumer = EventFeedRegistry::instance().acquire(WORKER_A, FEED);
        EXPECT_TRUE(producer->hasConsumer());
        EXPECT_THROW(EventFeedRegistry::instance().acquire(WORKER_A, FEED), Exception)
            << "Two consumers would divide the rows between them instead of each seeing the stream";
    }

    EXPECT_FALSE(producer->hasConsumer()) << "Releasing the handle has to free the feed for the next query";
    EXPECT_NO_THROW(std::ignore = EventFeedRegistry::instance().acquire(WORKER_A, FEED));
}

TEST_F(EventFeedTest, WorkersInOneProcessDoNotShareAFeedOfTheSameName)
{
    auto producerA = EventFeedRegistry::instance().create(WORKER_A, FEED, 8);
    auto producerB = EventFeedRegistry::instance().create(WORKER_B, FEED, 8);

    EXPECT_TRUE(producerA->tryPush("from A"));
    const auto consumerB = EventFeedRegistry::instance().acquire(WORKER_B, FEED);
    EXPECT_FALSE(consumerB->tryPop(POP_TIMEOUT).has_value()) << "Worker B must not observe the events of worker A";

    const auto consumerA = EventFeedRegistry::instance().acquire(WORKER_A, FEED);
    const auto row = consumerA->tryPop(POP_TIMEOUT);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row.value(), "from A");
}

/// The standalone starter identifies a worker by its data address, which is empty unless configured, while
/// queries are placed on the gRPC endpoint. A single worker therefore has to be found without a host match.
TEST_F(EventFeedTest, AcquiringFallsBackToTheOnlyFeedOfThatName)
{
    auto producer = EventFeedRegistry::instance().create(WORKER_A, FEED, 8);
    EXPECT_TRUE(producer->tryPush("a row"));

    const auto consumer = EventFeedRegistry::instance().acquire(UNKNOWN_WORKER, FEED);
    const auto row = consumer->tryPop(POP_TIMEOUT);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row.value(), "a row");
}

TEST_F(EventFeedTest, TheFallbackDoesNotGuessBetweenSeveralWorkers)
{
    auto producerA = EventFeedRegistry::instance().create(WORKER_A, FEED, 8);
    auto producerB = EventFeedRegistry::instance().create(WORKER_B, FEED, 8);

    EXPECT_THROW(EventFeedRegistry::instance().acquire(UNKNOWN_WORKER, FEED), Exception)
        << "Picking one of two workers would silently report the events of the wrong one";
}

TEST_F(EventFeedTest, DestroyingTheProducerFreesTheName)
{
    {
        const auto producer = EventFeedRegistry::instance().create(WORKER_A, FEED, 8);
    }
    /// A worker that is torn down and rebuilt inside one process, as the embedded backend does, must not
    /// collide with the feed of its predecessor.
    EXPECT_NO_THROW(std::ignore = EventFeedRegistry::instance().create(WORKER_A, FEED, 8));
}

/// NOLINTEND(bugprone-unchecked-optional-access)

}
