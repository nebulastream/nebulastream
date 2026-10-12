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
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Async/AsyncRecordLayout.hpp>
#include <Async/AsyncWiring.hpp>
#include <Async/HandoffChannel.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SourceRegistry.hpp>

/// The lifecycle edges of the two halves: stopping before starting, one half going away while
/// the other still runs, and stopping while slow work is still queued. The happy path is covered
/// end to end by the HTTP_CALL system test; these are the cases a query only reaches when
/// something else already went wrong.
namespace NES
{

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 4096;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 64;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

std::shared_ptr<BufferManager> makeBufferManager()
{
    return BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
}

UnqualifiedUnboundField field(const std::string& name, const DataType::Type type)
{
    return UnqualifiedUnboundField{Identifier::parse(name), type};
}

Schema<UnqualifiedUnboundField, Ordered> inputSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{field("reviewText", DataType::Type::VARSIZED)};
}

Schema<UnqualifiedUnboundField, Ordered> outputSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        field("reviewText", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)};
}

/// Only what a sink touches; repeatTask is counted so a test can tell a retry from a finished stop.
struct RecordingPipelineContext final : PipelineExecutionContext
{
    explicit RecordingPipelineContext(std::shared_ptr<BufferManager> bufferManager) : bufferManager(std::move(bufferManager)) { }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override { }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { ++repeatedTasks; }

    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    size_t repeatedTasks = 0;
};
}

class AsyncLifecycleTest : public ::testing::Test
{
public:
    std::shared_ptr<BufferManager> bufferManager = makeBufferManager();
    std::string channelId = UUIDToString(generateUUID());

    std::unique_ptr<Sink> makeSink(const size_t channelCapacity) const
    {
        const SinkCatalog sinkCatalog;
        const auto descriptor = sinkCatalog.getAnonymousSink(
            inputSchema(),
            Identifier::parse("Handoff"),
            Host("localhost"),
            {{Identifier::parse("channel"), channelId}, {Identifier::parse("channel_capacity"), std::to_string(channelCapacity)}},
            {});
        EXPECT_TRUE(descriptor.has_value());
        auto [controller, listener] = createBackpressureChannel();
        auto sink = SinkRegistry::instance().find("Handoff").value()(
            SinkRegistryArguments{.backpressureController = std::move(controller), .sinkDescriptor = descriptor.value()});
        return sink;
    }

    /// A consumer running the DelayExecutor: one record per call. `failOn`, when given, is the
    /// input text whose batch throws.
    std::unique_ptr<Source>
    makeSource(const std::chrono::milliseconds delay, const size_t maxConcurrency = 1, const std::string& failOn = {}) const
    {
        std::unordered_map<std::string, std::string> executorConfig{
            {"input_field", "reviewText"}, {"output_field", "sentiment"}, {"delay_ms", std::to_string(delay.count())}};
        if (!failOn.empty())
        {
            executorConfig.emplace("fail_on", failOn);
        }

        const SourceCatalog sourceCatalog;
        const auto descriptor = sourceCatalog.getAnonymousSource(
            Identifier::parse("Async"),
            outputSchema(),
            Host("localhost"),
            {{Identifier::parse(InputFormatterDescriptor::getTypeString()), "NATIVE"}},
            {{Identifier::parse("channel"), channelId},
             {Identifier::parse("executor_type"), "Delay"},
             {Identifier::parse("executor_config"), encodeConfig(executorConfig)},
             {Identifier::parse("input_schema"), encodeSchema(inputSchema())},
             {Identifier::parse("batch_size"), "1"},
             {Identifier::parse("max_concurrency"), std::to_string(maxConcurrency)}});
        EXPECT_TRUE(descriptor.has_value());
        return SourceRegistry::instance().find("Async").value()(SourceRegistryArguments{.sourceDescriptor = descriptor.value()});
    }

    /// A buffer of `records` input records, in the layout the producer half would hand over.
    TupleBuffer inputBuffer(const size_t records) const
    {
        const AsyncRecordLayout layout{inputSchema()};
        auto buffer = bufferManager->getBufferBlocking();
        for (size_t index = 0; index < records; ++index)
        {
            AsyncRecordWriter writer{layout, buffer, *bufferManager, index};
            writer.writeText(0, "review " + std::to_string(index));
        }
        buffer.setNumberOfTuples(records);
        /// Distinct, because BackpressureHandler recognises its retried buffer by sequence and chunk.
        buffer.setSequenceNumber(SequenceNumber(nextSequenceNumber++));
        buffer.setChunkNumber(INITIAL<ChunkNumber>);
        return buffer;
    }

    mutable SequenceNumber::Underlying nextSequenceNumber = SequenceNumber::INITIAL;
};

TEST_F(AsyncLifecycleTest, ConsumerCloseRejectsPushesAndReleasesQueuedBuffers)
{
    const auto availableBefore = bufferManager->getNumberOfAvailableBuffers();
    HandoffChannel channel{8};
    ASSERT_TRUE(channel.tryPush(bufferManager->getBufferBlocking()));
    ASSERT_TRUE(channel.tryPush(bufferManager->getBufferBlocking()));

    channel.closeConsumer();

    EXPECT_TRUE(channel.isConsumerClosed());
    EXPECT_FALSE(channel.isClosed());
    EXPECT_EQ(channel.size(), 0U);
    /// Nobody will read them, so they must not wait for the producer to let go of the channel.
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), availableBefore);
    EXPECT_FALSE(channel.tryPush(bufferManager->getBufferBlocking()));
}

TEST_F(AsyncLifecycleTest, SinkStoppedBeforeStartDoesNothing)
{
    /// A cancelled deployment stops pipelines that never started; this used to trip an INVARIANT.
    const auto sink = makeSink(4);
    RecordingPipelineContext context{bufferManager};

    sink->stop(context);

    EXPECT_EQ(context.repeatedTasks, 0U);
    EXPECT_EQ(HandoffChannelRegistry::find(channelId), nullptr);
}

TEST_F(AsyncLifecycleTest, SinkFailsOnceTheConsumerIsGone)
{
    const auto sink = makeSink(1);
    RecordingPipelineContext context{bufferManager};
    sink->start(context);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    sink->execute(inputBuffer(1), context);
    channel->closeConsumer();

    /// Retrying would hold the upstream sources under backpressure forever.
    EXPECT_ANY_THROW(sink->execute(inputBuffer(1), context));
    EXPECT_EQ(context.repeatedTasks, 0U);
}

TEST_F(AsyncLifecycleTest, SinkStopFinishesWhenTheConsumerIsGone)
{
    const auto sink = makeSink(1);
    RecordingPipelineContext context{bufferManager};
    sink->start(context);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    /// Fill the channel, then overflow it twice: the first overflow becomes the retried buffer,
    /// the second stays stashed for the stop to flush.
    sink->execute(inputBuffer(1), context);
    sink->execute(inputBuffer(1), context);
    sink->execute(inputBuffer(1), context);
    const auto repeatedBeforeStop = context.repeatedTasks;
    channel->closeConsumer();

    sink->stop(context);

    /// A repeated stop would retry its flush into a channel nobody drains, forever.
    EXPECT_EQ(context.repeatedTasks, repeatedBeforeStop);
}

TEST_F(AsyncLifecycleTest, SourceCloseTellsTheProducer)
{
    const auto source = makeSource(std::chrono::milliseconds{0});
    source->open(bufferManager);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    source->close();

    EXPECT_TRUE(channel->isConsumerClosed());
    EXPECT_FALSE(channel->tryPush(inputBuffer(1)));
}

TEST_F(AsyncLifecycleTest, SourceCloseDoesNotWorkOffQueuedBatches)
{
    constexpr auto Delay = std::chrono::milliseconds{200};
    constexpr size_t Records = 10;

    const auto source = makeSource(Delay);
    source->open(bufferManager);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    /// One buffer of ten single-record batches for a single worker: one call in flight, nine queued.
    ASSERT_TRUE(channel->tryPush(inputBuffer(Records)));
    std::this_thread::sleep_for(Delay / 2);

    const auto start = std::chrono::steady_clock::now();
    source->close();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    /// The call in flight cannot be interrupted, but the queued ones must not be made: working
    /// them off would take Records * Delay, i.e. two seconds here.
    EXPECT_LT(elapsed, Delay * 3) << "close() took " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << " ms";
}

/// A failing call must reach the source thread even while other calls are still finishing.
///
/// What this covers, and what it does not. A query whose operator failed under concurrency used to
/// stall outright, in roughly two runs of five: `recordFailure` says "nothing will complete any
/// more" by setting `inputExhausted`, which is what releases the source thread's wait, and a worker
/// that finished its own batch afterwards then *assigned* that flag from the queue state -- `false`,
/// with the failed buffer still pending. Every other worker had already left its loop on the stop
/// that `recordFailure` requests, so nobody set it again and the source waited for a buffer that no
/// longer had a worker. The query neither failed nor finished.
///
/// The window is narrow: the clearing has to land between the source thread being woken and its
/// re-acquiring the mutex to re-check the predicate. This test does not force that window -- it
/// passes against the unfixed source as well, which was checked. Reproducing it needed a real query
/// whose operator fails under concurrency -- an unreachable endpoint with several calls in flight --
/// which hung in two of five runs before the fix and in none of thirty after it.
///
/// So what this test is for is the path itself: one call of sixteen fails over four workers while
/// the others finish, and the source has to come out with that failure rather than with end of
/// stream, a short result, or a wait. The stall is prevented by construction instead -- the flag is
/// only ever set, and `failure` is part of every wait predicate.
TEST_F(AsyncLifecycleTest, AFailingCallReleasesTheSourceWhileOtherCallsFinish)
{
    constexpr auto Delay = std::chrono::milliseconds{100};
    constexpr size_t Records = 16;
    constexpr size_t Workers = 4;

    const auto source = makeSource(Delay, Workers, "review 0");
    source->open(bufferManager);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    ASSERT_TRUE(channel->tryPush(inputBuffer(Records)));
    /// The producer is done, so nothing but the failure can end this source.
    channel->close();

    auto output = bufferManager->getBufferBlocking();
    const std::stop_source stop;
    const auto start = std::chrono::steady_clock::now();
    EXPECT_ANY_THROW(std::ignore = source->fillTupleBuffer(output, stop.get_token()));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    /// Generous: the point is that it returns at all. Working off all sixteen batches would take
    /// Records / Workers * Delay even if it did not hang.
    EXPECT_LT(elapsed, Delay * 20) << "fillTupleBuffer took " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                                   << " ms";
    source->close();
}

/// A source holds its channel from the moment it is built, not from when it is opened. Opening
/// happens on the source thread, some time after its plan was started; building happens while the
/// plan is started, and the splitter starts the consumer's plan before its producer's. Only that
/// ordering keeps a producer that finishes early from taking the channel down with it.
TEST_F(AsyncLifecycleTest, ASourceHoldsItsChannelFromConstruction)
{
    EXPECT_EQ(HandoffChannelRegistry::find(channelId), nullptr);
    const auto source = makeSource(std::chrono::milliseconds{0});
    EXPECT_NE(HandoffChannelRegistry::find(channelId), nullptr) << "the channel must exist before open() runs";
}

/// A deployment can be cancelled after a source was built but before it was opened. Its producer
/// must still learn that nobody will read, or it retries a full channel forever.
TEST_F(AsyncLifecycleTest, ASourceDestroyedUnopenedTellsTheProducer)
{
    std::shared_ptr<HandoffChannel> channel;
    {
        const auto source = makeSource(std::chrono::milliseconds{0});
        channel = HandoffChannelRegistry::find(channelId);
        ASSERT_NE(channel, nullptr);
        EXPECT_FALSE(channel->isConsumerClosed());
    }
    EXPECT_TRUE(channel->isConsumerClosed());
}

}
