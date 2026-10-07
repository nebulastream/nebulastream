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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
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
#include <Time/Timestamp.hpp>
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
/// end to end by the semantic system tests; these are the cases a query only reaches when
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

    struct SourceOptions
    {
        std::chrono::milliseconds delay{0};
        /// Records whose text starts with this are dropped by the executor; empty drops nothing.
        std::string dropPrefix;
        size_t maxConcurrency = 1;
        bool preserveOrder = true;
    };

    /// A consumer running the DelayExecutor: one record per call, one call at a time.
    std::unique_ptr<Source> makeSource(const std::chrono::milliseconds delay) const
    {
        return makeSource(SourceOptions{.delay = delay, .dropPrefix = {}, .maxConcurrency = 1, .preserveOrder = true});
    }

    std::unique_ptr<Source> makeSource(const SourceOptions& options) const
    {
        const SourceCatalog sourceCatalog;
        const auto descriptor = sourceCatalog.getAnonymousSource(
            Identifier::parse("Async"),
            outputSchema(),
            Host("localhost"),
            {{Identifier::parse(InputFormatterDescriptor::getTypeString()), "NATIVE"}},
            {{Identifier::parse("channel"), channelId},
             {Identifier::parse("executor_type"), "Delay"},
             {Identifier::parse("executor_config"),
              encodeConfig(
                  {{"input_field", "reviewText"},
                   {"output_field", "sentiment"},
                   {"delay_ms", std::to_string(options.delay.count())},
                   {"drop_prefix", options.dropPrefix}})},
             {Identifier::parse("input_schema"), encodeSchema(inputSchema())},
             {Identifier::parse("batch_size"), "1"},
             {Identifier::parse("max_concurrency"), std::to_string(options.maxConcurrency)},
             {Identifier::parse("preserve_order"), options.preserveOrder ? "true" : "false"}});
        EXPECT_TRUE(descriptor.has_value());
        return SourceRegistry::instance().find("Async").value()(SourceRegistryArguments{.sourceDescriptor = descriptor.value()});
    }

    /// A buffer of `records` input records, in the layout the producer half would hand over.
    TupleBuffer inputBuffer(const size_t records) const
    {
        std::vector<std::string> texts;
        for (size_t index = 0; index < records; ++index)
        {
            texts.push_back("review " + std::to_string(index));
        }
        return inputBuffer(texts);
    }

    TupleBuffer inputBuffer(const std::vector<std::string>& texts) const
    {
        const AsyncRecordLayout layout{inputSchema()};
        auto buffer = bufferManager->getBufferBlocking();
        for (size_t index = 0; index < texts.size(); ++index)
        {
            AsyncRecordWriter writer{layout, buffer, *bufferManager, index};
            writer.writeText(0, texts[index]);
        }
        buffer.setNumberOfTuples(texts.size());
        buffer.setOriginId(OriginId(7));
        buffer.setWatermark(Timestamp(1000 * (nextSequenceNumber + 1)));
        buffer.setLastChunk(true);
        /// Distinct, because BackpressureHandler recognises its retried buffer by sequence and chunk.
        buffer.setSequenceNumber(SequenceNumber(nextSequenceNumber++));
        buffer.setChunkNumber(INITIAL<ChunkNumber>);
        return buffer;
    }

    /// One emitted buffer, decoded: what downstream would see.
    struct Emitted
    {
        std::vector<std::string> texts;
        SequenceNumber sequence = INVALID<SequenceNumber>;
        ChunkNumber chunk = INVALID<ChunkNumber>;
        bool lastChunk = false;
        Timestamp watermark{Timestamp::INVALID_VALUE};
    };

    Emitted emitOne(Source& source) const
    {
        auto buffer = bufferManager->getBufferBlocking();
        const std::stop_source stop;
        const auto result = source.fillTupleBuffer(buffer, stop.get_token());
        EXPECT_FALSE(result.isEoS());
        const AsyncRecordLayout layout{outputSchema()};
        Emitted emitted{
            .texts = {},
            .sequence = buffer.getSequenceNumber(),
            .chunk = buffer.getChunkNumber(),
            .lastChunk = buffer.isLastChunk(),
            .watermark = buffer.getWatermark()};
        EXPECT_EQ(buffer.getNumberOfTuples(), result.getNumberOfBytes());
        for (size_t index = 0; index < buffer.getNumberOfTuples(); ++index)
        {
            emitted.texts.push_back(AsyncRecordView{layout, buffer, index}.readAsText(1));
        }
        return emitted;
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

/// A filtering executor drops records; the survivors keep their order and the buffer its metadata.
TEST_F(AsyncLifecycleTest, DroppedRecordsAreSkipped)
{
    const auto source = makeSource(SourceOptions{.delay = {}, .dropPrefix = "drop", .maxConcurrency = 1, .preserveOrder = true});
    source->open(bufferManager);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    ASSERT_TRUE(channel->tryPush(inputBuffer({"keep a", "drop b", "keep c", "drop d"})));
    const auto emitted = emitOne(*source);

    EXPECT_EQ(emitted.texts, (std::vector<std::string>{"KEEP A", "KEEP C"}));
    EXPECT_EQ(emitted.sequence, SequenceNumber(SequenceNumber::INITIAL));
    EXPECT_EQ(emitted.chunk, INITIAL<ChunkNumber>);
    EXPECT_TRUE(emitted.lastChunk);
    EXPECT_EQ(emitted.watermark, Timestamp(1000 * (SequenceNumber::INITIAL + 1)));
    source->close();
}

/// Nothing survives, yet the buffer must still go out: it alone carries the sequence number, the
/// watermark and the last-chunk flag, without which a downstream window would wait forever.
TEST_F(AsyncLifecycleTest, AFullyDroppedBufferStillCarriesItsMetadata)
{
    const auto source = makeSource(SourceOptions{.delay = {}, .dropPrefix = "drop", .maxConcurrency = 1, .preserveOrder = true});
    source->open(bufferManager);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    ASSERT_TRUE(channel->tryPush(inputBuffer({"drop a", "drop b"})));
    ASSERT_TRUE(channel->tryPush(inputBuffer({"keep c"})));

    const auto empty = emitOne(*source);
    EXPECT_TRUE(empty.texts.empty());
    EXPECT_EQ(empty.sequence, SequenceNumber(SequenceNumber::INITIAL));
    EXPECT_EQ(empty.chunk, INITIAL<ChunkNumber>);
    EXPECT_TRUE(empty.lastChunk);
    EXPECT_EQ(empty.watermark, Timestamp(1000 * (SequenceNumber::INITIAL + 1)));

    const auto next = emitOne(*source);
    EXPECT_EQ(next.texts, (std::vector<std::string>{"KEEP C"}));
    EXPECT_EQ(next.sequence, SequenceNumber(SequenceNumber::INITIAL + 1));
    EXPECT_TRUE(next.lastChunk);
    source->close();
}

/// Dropped records take no output space, so one output buffer covers more input than it holds; and
/// dropped records after a full output are consumed with it, so they cost no extra, empty chunk.
TEST_F(AsyncLifecycleTest, DropsAcrossChunks)
{
    const auto outputCapacity = AsyncRecordLayout{outputSchema()}.capacity(bufferManager->getBufferSize());
    const auto inputCapacity = AsyncRecordLayout{inputSchema()}.capacity(bufferManager->getBufferSize());
    ASSERT_GT(inputCapacity, outputCapacity) << "the test needs input records smaller than output records";

    const auto source = makeSource(SourceOptions{.delay = {}, .dropPrefix = "drop", .maxConcurrency = 1, .preserveOrder = true});
    source->open(bufferManager);
    const auto channel = HandoffChannelRegistry::find(channelId);
    ASSERT_NE(channel, nullptr);

    /// Exactly one output buffer of kept records, then a dropped tail: one chunk, and the last.
    std::vector<std::string> texts;
    for (size_t index = 0; index < outputCapacity; ++index)
    {
        texts.push_back("keep " + std::to_string(index));
    }
    const auto tail = std::min<size_t>(3, inputCapacity - outputCapacity);
    for (size_t index = 0; index < tail; ++index)
    {
        texts.push_back("drop " + std::to_string(index));
    }
    ASSERT_TRUE(channel->tryPush(inputBuffer(texts)));

    const auto first = emitOne(*source);
    EXPECT_EQ(first.texts.size(), outputCapacity);
    EXPECT_EQ(first.texts.back(), "KEEP " + std::to_string(outputCapacity - 1));
    EXPECT_EQ(first.chunk, INITIAL<ChunkNumber>);
    EXPECT_TRUE(first.lastChunk);

    /// One kept record more than fits, with drops in between: the overflow becomes a second chunk.
    std::vector<std::string> overflow;
    for (size_t index = 0; index <= outputCapacity && overflow.size() < inputCapacity; ++index)
    {
        overflow.push_back("keep " + std::to_string(index));
        if (overflow.size() < inputCapacity)
        {
            overflow.push_back("drop " + std::to_string(index));
        }
    }
    ASSERT_TRUE(channel->tryPush(inputBuffer(overflow)));
    const auto keptInOverflow
        = static_cast<size_t>(std::ranges::count_if(overflow, [](const std::string& text) { return text.starts_with("keep"); }));
    if (keptInOverflow > outputCapacity)
    {
        const auto head = emitOne(*source);
        EXPECT_EQ(head.texts.size(), outputCapacity);
        EXPECT_EQ(head.chunk, INITIAL<ChunkNumber>);
        EXPECT_FALSE(head.lastChunk);
        const auto rest = emitOne(*source);
        EXPECT_EQ(rest.texts.size(), keptInOverflow - outputCapacity);
        EXPECT_EQ(rest.sequence, head.sequence);
        EXPECT_EQ(rest.chunk, ChunkNumber(ChunkNumber::INITIAL + 1));
        EXPECT_TRUE(rest.lastChunk);
    }
    else
    {
        /// Input records too large for the interleaving to overflow; everything fits one chunk.
        const auto combined = emitOne(*source);
        EXPECT_EQ(combined.texts.size(), keptInOverflow);
        EXPECT_TRUE(combined.lastChunk);
    }
    source->close();
}

/// With several calls in flight, PRESERVE_ORDER decides whether buffers leave in input order; drops
/// must not change which records survive either way.
TEST_F(AsyncLifecycleTest, DropsRespectOrdering)
{
    for (const bool preserveOrder : {true, false})
    {
        SCOPED_TRACE(preserveOrder ? "ordered" : "unordered");
        channelId = UUIDToString(generateUUID());
        const auto source = makeSource(SourceOptions{
            .delay = std::chrono::milliseconds{5}, .dropPrefix = "drop", .maxConcurrency = 4, .preserveOrder = preserveOrder});
        source->open(bufferManager);
        const auto channel = HandoffChannelRegistry::find(channelId);
        ASSERT_NE(channel, nullptr);

        constexpr size_t Buffers = 8;
        const auto firstSequence = nextSequenceNumber;
        for (size_t buffer = 0; buffer < Buffers; ++buffer)
        {
            const auto tag = std::to_string(buffer);
            ASSERT_TRUE(channel->tryPush(inputBuffer({"keep " + tag + "a", "drop " + tag, "keep " + tag + "b"})));
        }

        std::vector<SequenceNumber::Underlying> sequences;
        std::vector<std::string> survivors;
        for (size_t buffer = 0; buffer < Buffers; ++buffer)
        {
            const auto emitted = emitOne(*source);
            EXPECT_TRUE(emitted.lastChunk);
            ASSERT_EQ(emitted.texts.size(), 2U);
            sequences.push_back(emitted.sequence.getRawValue());
            survivors.insert(survivors.end(), emitted.texts.begin(), emitted.texts.end());
        }

        if (preserveOrder)
        {
            EXPECT_TRUE(std::ranges::is_sorted(sequences));
        }
        std::ranges::sort(sequences);
        for (size_t buffer = 0; buffer < Buffers; ++buffer)
        {
            EXPECT_EQ(sequences[buffer], firstSequence + buffer);
        }
        std::ranges::sort(survivors);
        EXPECT_EQ(std::ranges::count_if(survivors, [](const std::string& text) { return text.starts_with("DROP"); }), 0);
        EXPECT_EQ(survivors.size(), 2 * Buffers);
        source->close();
    }
}

}
