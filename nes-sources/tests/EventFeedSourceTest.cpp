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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Feeds/EventFeed.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/LogicalSource.hpp>
#include <Sources/SourceDescriptor.hpp>
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
constexpr std::chrono::milliseconds EMIT_TIMEOUT{5000};
constexpr std::chrono::milliseconds STOP_TIMEOUT{5000};
constexpr size_t DEFAULT_MAX_INFLIGHT_BUFFERS = 10;
constexpr uint32_t POOLED_BUFFER_SIZE = 8192;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 1024;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;
constexpr size_t FEED_CAPACITY = 16;

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

Schema<UnqualifiedUnboundField, Ordered> taskEventSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("event_type"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("ts_us"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("thread_id"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("query_id"), DataType::Type::VARSIZED},
        UnqualifiedUnboundField{Identifier::parse("pipeline_id"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("task_id"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("tuples"), DataType::Type::UINT64}};
}

/// The leading columns of what 'BufferStatisticListener' publishes. The source does not interpret the row,
/// so the test only has to agree with itself on how many columns there are.
Schema<UnqualifiedUnboundField, Ordered> bufferEventSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("ts_us"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("interval_ms"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("pooled_total"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("pooled_available"), DataType::Type::UINT64}};
}
}

class EventFeedSourceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("EventFeedSourceTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup EventFeedSourceTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

/// clang tidy doesn't recognize the ASSERT_TRUE guarding the optional accesses below
/// NOLINTBEGIN(bugprone-unchecked-optional-access)

namespace
{
/// The path a query takes: the descriptor goes through the SourceProvider, which resolves
/// the source type in the SourceRegistry, and the resulting source hands the feed's rows to the query
/// engine. Which feed a source reads follows from its type alone, so each source type is checked against
/// the feed it is supposed to find.
void expectSourceReadsFeed(
    const std::string& sourceType,
    const std::string_view feedName,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    const std::vector<std::string>& rows)
{
    const Host host{"localhost:8080"};
    auto producer = EventFeedRegistry::instance().create(host, feedName, FEED_CAPACITY);
    std::string expected;
    for (const auto& row : rows)
    {
        EXPECT_TRUE(producer->tryPush(row));
        expected.append(row).push_back('\n');
    }

    const LogicalSource logicalSource{Identifier::parse("stats"), schema};
    const auto descriptor = SourceDescriptor::create(
        PhysicalSourceId{1},
        logicalSource,
        Identifier::parse(sourceType),
        host,
        {{Identifier::parse("flush_interval_ms"), "10"}},
        {{Identifier::parse("type"), "CSV"}},
        false);
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
    EXPECT_TRUE(producer->hasConsumer()) << "A running query has to hold the feed";

    /// An empty feed must not terminate the source, it only means that nothing has happened yet.
    EXPECT_EQ(source->tryStop(STOP_TIMEOUT), SourceReturnType::TryStopResult::SUCCESS);
}
}

TEST_F(EventFeedSourceTest, EngineEventsSourceEmitsWhatTheWorkerPushedIntoItsFeed)
{
    expectSourceReadsFeed(
        "EngineEvents", FeedName::ENGINE_EVENTS, taskEventSchema(), {"TASK_START,1000,2,1,4,5,64", "TASK_DONE,1001,2,1,4,5,0"});
}

TEST_F(EventFeedSourceTest, BufferEventsSourceEmitsWhatTheWorkerPushedIntoItsFeed)
{
    expectSourceReadsFeed("BufferEvents", FeedName::BUFFER_EVENTS, bufferEventSchema(), {"1000,100,1024,1020", "1100,100,1024,1017"});
}

/// NOLINTEND(bugprone-unchecked-optional-access)

}
