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

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/InProcessFeed.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
constexpr std::chrono::milliseconds POP_TIMEOUT{100};
constexpr std::chrono::milliseconds EMIT_TIMEOUT{5000};
constexpr std::chrono::milliseconds STOP_TIMEOUT{5000};
constexpr size_t DEFAULT_MAX_INFLIGHT_BUFFERS = 10;
constexpr uint32_t POOLED_BUFFER_SIZE = 8192;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 1024;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

/// Collects the bytes of every emitted buffer, so that a test can wait until the source has handed on
/// everything that was pushed into its feed.
class EmitRecorder
{
public:
    void record(const std::string_view bytes)
    {
        const std::lock_guard lock(mutex);
        emitted.append(bytes);
        block.notify_all();
    }

    /// Waits until the concatenation of all emitted buffers equals 'expected', and returns what was
    /// actually emitted so that a failing test reports the difference.
    std::string waitFor(const std::string& expected)
    {
        std::unique_lock lock(mutex);
        block.wait_for(lock, EMIT_TIMEOUT, [&] { return emitted.size() >= expected.size(); });
        return emitted;
    }

private:
    std::mutex mutex;
    std::condition_variable block;
    std::string emitted;
};

/// The columns that 'BufferStatisticListener' publishes, all UINT64. Kept here rather than shared with the
/// listener because nes-sources must not depend on the worker; 'BufferStatisticListenerTest' is what guards
/// the listener's own schema against drifting from the rows it renders.
constexpr std::array<std::string_view, 15> BUFFER_STATISTICS_COLUMNS{
    "ts_us",
    "interval_ms",
    "pooled_total",
    "pooled_available",
    "pooled_available_min",
    "pooled_acquired",
    "pooled_recycled",
    "pooled_request_failures",
    "unpooled_allocated",
    "unpooled_bytes_requested",
    "unpooled_bytes_in_use",
    "unpooled_chunks_allocated",
    "unpooled_chunks_released",
    "unpooled_request_failures",
    "rows_dropped"};

Schema<UnqualifiedUnboundField, Ordered> bufferStatisticsSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        BUFFER_STATISTICS_COLUMNS
        | std::views::transform([](const std::string_view column)
                                { return UnqualifiedUnboundField{Identifier::parse(std::string{column}), DataType::Type::UINT64}; })};
}
}

class InProcessSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("InProcessSourceTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InProcessSourceTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

/// clang tidy doesn't recognize the ASSERT_TRUE guarding the optional accesses below
/// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST_F(InProcessSourceTest, FeedDropsRowsWhenFull)
{
    InProcessFeed feed(2);
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

TEST_F(InProcessSourceTest, RegistryHandsOutTheSameFeedForTheSameName)
{
    auto& registry = InProcessFeedRegistry::instance();
    const auto producerView = registry.getOrCreate("shared_feed", 4);
    const auto consumerView = registry.getOrCreate("shared_feed", 64);

    EXPECT_EQ(producerView.get(), consumerView.get()) << "Producer and consumer have to end up on the same feed";
    EXPECT_EQ(consumerView->capacity(), 4) << "The capacity of the first caller wins";
    EXPECT_NE(registry.getOrCreate("other_feed", 4).get(), producerView.get());
}

/// The path a query takes: the descriptor goes through the catalog and the SourceProvider, which resolves
/// 'InProcess' in the SourceRegistry, and the resulting source hands the feed's rows to the query engine.
TEST_F(InProcessSourceTest, SourceEmitsWhatWasPushedIntoItsFeed)
{
    const auto feed = InProcessFeedRegistry::instance().getOrCreate("query_feed", 16);
    const std::string firstRow = "1755431000123,100,4096,3871,3402,18422,18401,0,12,49152,98304,1,0,0,0";
    const std::string secondRow = "1755431000223,100,4096,4096,3871,18400,18421,0,0,0,0,0,1,0,0";
    EXPECT_TRUE(feed->tryPush(firstRow));
    EXPECT_TRUE(feed->tryPush(secondRow));
    const std::string expected = firstRow + "\n" + secondRow + "\n";

    SourceCatalog catalog;
    const auto logicalSource = catalog.addLogicalSource(Identifier::parse("bufferStats"), bufferStatisticsSchema());
    ASSERT_TRUE(logicalSource.has_value());

    const auto descriptor = catalog.addPhysicalSource(
        *logicalSource,
        Identifier::parse("InProcess"),
        Host("localhost"),
        {{Identifier::parse("feed_name"), "query_feed"}, {Identifier::parse("flush_interval_ms"), "10"}},
        {{Identifier::parse("type"), "CSV"}});
    ASSERT_TRUE(descriptor.has_value()) << "Creating the physical source failed";

    auto bufferManager = BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
    const SourceProvider provider(DEFAULT_MAX_INFLIGHT_BUFFERS, bufferManager);
    auto [backpressureController, backpressureListener] = createBackpressureChannel();
    const auto source = provider.lower(INITIAL<OriginId>, std::move(backpressureListener), descriptor.value());
    ASSERT_NE(source, nullptr);

    EmitRecorder recorder;
    ASSERT_TRUE(source->start(
        [&recorder](const OriginId, SourceReturnType::SourceReturnType returned, const std::stop_token&)
        {
            if (auto* data = std::get_if<SourceReturnType::Data>(&returned))
            {
                const auto area = data->buffer.getAvailableMemoryArea<const char>();
                recorder.record(std::string_view{area.data(), data->buffer.getNumberOfTuples()});
            }
            return SourceReturnType::EmitResult::SUCCESS;
        }));

    EXPECT_EQ(recorder.waitFor(expected), expected);

    /// An empty feed must not terminate the source, it only means that nothing has happened yet.
    EXPECT_EQ(source->tryStop(STOP_TIMEOUT), SourceReturnType::TryStopResult::SUCCESS);
}

/// NOLINTEND(bugprone-unchecked-optional-access)

}
