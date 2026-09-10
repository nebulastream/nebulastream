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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <Feeds/EventFeed.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferProviderStatisticListener.hpp>
#include <gtest/gtest.h>
#include <BufferStatisticListener.hpp>

namespace NES
{

namespace
{
constexpr size_t FEED_CAPACITY = 16;
constexpr std::chrono::milliseconds FLUSH_INTERVAL{100};

/// The worker a listener publishes for. Which worker it is does not matter here, only that producer and
/// consumer agree on it, the way a source and its worker do.
const Host WORKER{"localhost:8080"};

/// Index of every column of the published row, so an assertion reads as the column name rather than a number.
enum Column : std::uint8_t
{
    TS_US = 0,
    INTERVAL_MS,
    POOLED_TOTAL,
    POOLED_AVAILABLE,
    POOLED_AVAILABLE_MIN,
    POOLED_ACQUIRED,
    POOLED_RECYCLED,
    POOLED_REQUEST_FAILURES,
    UNPOOLED_ALLOCATED,
    UNPOOLED_BYTES_REQUESTED,
    UNPOOLED_BYTES_IN_USE,
    UNPOOLED_CHUNKS_ALLOCATED,
    UNPOOLED_CHUNKS_RELEASED,
    UNPOOLED_REQUEST_FAILURES,
    ROWS_DROPPED,
    COLUMN_COUNT
};

std::vector<std::string> splitRow(const std::string_view row)
{
    std::vector<std::string> columns;
    size_t start = 0;
    while (start <= row.size())
    {
        const auto separator = row.find(',', start);
        if (separator == std::string_view::npos)
        {
            columns.emplace_back(row.substr(start));
            break;
        }
        columns.emplace_back(row.substr(start, separator - start));
        start = separator + 1;
    }
    return columns;
}
}

class BufferStatisticListenerTest : public ::testing::Test
{
protected:
    /// Feeds live in a process-global registry, so a test must not inherit the feed of another one.
    void TearDown() override { EventFeedRegistry::instance().clear(); }
};

/// The declared schema and the rendered row have to stay in lockstep, or a query over the feed silently reads
/// the wrong column.
TEST_F(BufferStatisticListenerTest, SchemaMatchesTheRenderedRow)
{
    BufferStatisticListener listener(WORKER, FEED_CAPACITY, FLUSH_INTERVAL);

    const auto declared = splitRow(BufferStatisticListener::SCHEMA);
    const auto rendered = splitRow(listener.flushRow(std::chrono::system_clock::now()));

    EXPECT_EQ(declared.size(), COLUMN_COUNT);
    EXPECT_EQ(rendered.size(), COLUMN_COUNT);
    EXPECT_EQ(declared.at(POOLED_ACQUIRED), "pooled_acquired") << "The Column enum drifted from the schema";
    EXPECT_EQ(declared.at(ROWS_DROPPED), "rows_dropped");
}

/// The two tests below feed synthetic events whose payload values are deliberately arbitrary and distinct:
/// naming them would hide the very thing being asserted, namely which value lands in which column.
/// NOLINTBEGIN(readability-magic-numbers)

/// One row summarises everything the buffer providers reported since the previous row.
TEST_F(BufferStatisticListenerTest, AggregatesAnIntervalIntoOneRow)
{
    BufferStatisticListener listener(WORKER, FEED_CAPACITY, FLUSH_INTERVAL);

    listener.onEvent(BufferPoolCreated{64, 4096, 1024});
    listener.onEvent(PooledBufferAcquired{63});
    listener.onEvent(PooledBufferAcquired{62});
    listener.onEvent(PooledBufferRecycled{63});
    listener.onEvent(PooledBufferRequestFailed{std::chrono::milliseconds{10}});
    listener.onEvent(UnpooledChunkAllocated{8192, 8192});
    listener.onEvent(UnpooledBufferAllocated{1024, 8192});
    listener.onEvent(UnpooledChunkReleased{8192, 0});
    listener.onEvent(UnpooledBufferRequestFailed{4096, 0});

    const auto row = splitRow(listener.flushRow(std::chrono::system_clock::now()));
    ASSERT_EQ(row.size(), COLUMN_COUNT);

    EXPECT_EQ(row.at(POOLED_TOTAL), "64") << "Latched from BufferPoolCreated";
    EXPECT_EQ(row.at(POOLED_AVAILABLE), "63") << "The gauge holds the most recently observed fill level";
    EXPECT_EQ(row.at(POOLED_AVAILABLE_MIN), "62") << "The low water mark of the interval, not the final level";
    EXPECT_EQ(row.at(POOLED_ACQUIRED), "2");
    EXPECT_EQ(row.at(POOLED_RECYCLED), "1");
    EXPECT_EQ(row.at(POOLED_REQUEST_FAILURES), "1");
    EXPECT_EQ(row.at(UNPOOLED_ALLOCATED), "1");
    EXPECT_EQ(row.at(UNPOOLED_BYTES_REQUESTED), "1024");
    EXPECT_EQ(row.at(UNPOOLED_BYTES_IN_USE), "0") << "The gauge holds the most recently reported value";
    EXPECT_EQ(row.at(UNPOOLED_CHUNKS_ALLOCATED), "1");
    EXPECT_EQ(row.at(UNPOOLED_CHUNKS_RELEASED), "1");
    EXPECT_EQ(row.at(UNPOOLED_REQUEST_FAILURES), "1");
    EXPECT_EQ(row.at(ROWS_DROPPED), "0");
    EXPECT_NE(row.at(TS_US), "0");
}

/// A flush has to hand over the counters rather than copy them, otherwise every row would restate the history
/// of the whole run.
TEST_F(BufferStatisticListenerTest, ResetsCountersButKeepsGauges)
{
    BufferStatisticListener listener(WORKER, FEED_CAPACITY, FLUSH_INTERVAL);

    listener.onEvent(BufferPoolCreated{64, 4096, 1024});
    listener.onEvent(PooledBufferAcquired{7});
    listener.onEvent(PooledBufferRecycled{9});
    const auto now = std::chrono::system_clock::now();
    listener.flushRow(now);

    const auto second = splitRow(listener.flushRow(now + FLUSH_INTERVAL));
    ASSERT_EQ(second.size(), COLUMN_COUNT);

    EXPECT_EQ(second.at(POOLED_ACQUIRED), "0");
    EXPECT_EQ(second.at(POOLED_RECYCLED), "0");
    EXPECT_EQ(second.at(UNPOOLED_BYTES_REQUESTED), "0");
    EXPECT_EQ(second.at(POOLED_TOTAL), "64") << "The pool did not change size";
    EXPECT_EQ(second.at(POOLED_AVAILABLE), "9") << "A gauge survives the flush";
    EXPECT_EQ(second.at(POOLED_AVAILABLE_MIN), "9") << "An interval without any event falls back to the gauge";
    EXPECT_EQ(second.at(INTERVAL_MS), std::to_string(FLUSH_INTERVAL.count())) << "A row reports the interval it actually covers";
}

/// NOLINTEND(readability-magic-numbers)

/// The published rows have to end up on the feed a BufferEvents source reads, which is what makes them
/// queryable.
TEST_F(BufferStatisticListenerTest, PublishesRowsIntoItsFeed)
{
    constexpr std::chrono::milliseconds fastInterval{20};
    constexpr std::chrono::milliseconds popTimeout{5000};

    BufferStatisticListener listener(WORKER, FEED_CAPACITY, fastInterval);
    listener.onEvent(PooledBufferAcquired{5});

    /// A source placed on this worker resolves the very same feed, so this is what a query would observe.
    /// It attaches before the listener starts, because nothing is published while nobody reads.
    const auto consumer = EventFeedRegistry::instance().acquire(WORKER, FeedName::BUFFER_EVENTS);
    listener.start();

    const auto row = consumer->tryPop(popTimeout);
    ASSERT_TRUE(row.has_value()) << "The flushing thread published nothing";
    EXPECT_EQ(splitRow(*row).size(), COLUMN_COUNT);

    /// An idle interval still produces a row, so that the series has no gaps.
    EXPECT_TRUE(consumer->tryPop(popTimeout).has_value());
    EXPECT_EQ(consumer->droppedRows(), 0);
}

/// Until a query reads the feed there is nobody to read the rows, and publishing them anyway would fill the
/// feed with a backlog that the first query then has to work through before it sees the present.
TEST_F(BufferStatisticListenerTest, PublishesNothingWhileNobodyReads)
{
    /// Deliberately far too small for the number of intervals below, so that a listener which published
    /// regardless of whether anyone reads would overflow the feed and be caught by the drop count.
    constexpr size_t tinyCapacity = 4;
    constexpr std::chrono::milliseconds fastInterval{5};
    constexpr std::chrono::milliseconds unreadFor{100};
    constexpr std::chrono::milliseconds popTimeout{5000};

    BufferStatisticListener listener(WORKER, tinyCapacity, fastInterval);
    listener.start();
    std::this_thread::sleep_for(unreadFor);

    const auto consumer = EventFeedRegistry::instance().acquire(WORKER, FeedName::BUFFER_EVENTS);
    EXPECT_EQ(consumer->droppedRows(), 0) << "Rows were published into a feed nobody was reading";

    /// The listener is merely waiting for a reader, not broken: rows resume as soon as one attaches.
    EXPECT_TRUE(consumer->tryPop(popTimeout).has_value());
}

}
