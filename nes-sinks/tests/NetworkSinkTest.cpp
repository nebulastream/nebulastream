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

/// Tests how the NetworkSink reacts to the reconfiguration markers of ISQD: it connects to a new receiver for an update and forwards the
/// marker or detaches when its query is drained. The sink sends to NetworkSources over the in-memory transport. The test drives the sink
/// itself and plays the role of the task queue for the tasks the sink repeats. The harness follows the NetworkSinkTest of the
/// marker-based-reconfiguration branch.

#include <Sinks/NetworkSink.hpp>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <future>
#include <memory>
#include <numeric>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/NetworkSource.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>
#include <ReconfigurationState.hpp>

namespace NES
{
namespace
{
constexpr auto TIMEOUT = std::chrono::seconds(10);

/// Collects the tasks the sink repeats instead of scheduling them.
struct TestPipelineExecutionContext final : PipelineExecutionContext
{
    TestPipelineExecutionContext(ReconfigurationState& reconfigurationState, std::shared_ptr<BufferManager> bufferManager)
        : reconfigurationState(reconfigurationState), bufferManager(std::move(bufferManager))
    {
    }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return false; }

    void repeatTask(const TupleBuffer& buffer, std::chrono::milliseconds) override { repeated.push_back(buffer); }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>&) override { }

    [[nodiscard]] ReconfigurationState& getReconfigurationState() override { return reconfigurationState; }

    ReconfigurationState& reconfigurationState; ///NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::deque<TupleBuffer> repeated;
};

/// The sequence numbers of the buffers a source delivers until its stream ends, and how it ended.
struct Received
{
    std::vector<uint64_t> sequenceNumbers;
    std::optional<ReconfigurationMarker> marker;
};

Received receiveUntilEndOfStream(NetworkSource& source, AbstractBufferProvider& bufferProvider)
{
    Received received;
    const std::stop_source neverStopped;
    while (true)
    {
        auto buffer = bufferProvider.getBufferBlocking();
        const auto result = source.fillTupleBuffer(buffer, neverStopped.get_token());
        if (result.isMarker())
        {
            received.marker = ReconfigurationMarker::deserialize(result.getMarker());
            return received;
        }
        if (result.isEoS())
        {
            return received;
        }
        received.sequenceNumbers.push_back(buffer.getSequenceNumber().getRawValue());
    }
}

std::vector<uint64_t> receive(NetworkSource& source, AbstractBufferProvider& bufferProvider, const size_t count)
{
    std::vector<uint64_t> sequenceNumbers;
    const std::stop_source neverStopped;
    while (sequenceNumbers.size() < count)
    {
        auto buffer = bufferProvider.getBufferBlocking();
        const auto result = source.fillTupleBuffer(buffer, neverStopped.get_token());
        if (result.isEoS() or result.isMarker())
        {
            ADD_FAILURE() << "The stream ended after " << sequenceNumbers.size() << " of " << count << " buffers";
            break;
        }
        sequenceNumbers.push_back(buffer.getSequenceNumber().getRawValue());
    }
    return sequenceNumbers;
}

std::vector<uint64_t> range(const uint64_t first, const uint64_t last)
{
    std::vector<uint64_t> values(last - first + 1);
    std::iota(values.begin(), values.end(), first);
    return values;
}

NetworkServiceOptions options(const uint32_t receiverQueueSize)
{
    return NetworkServiceOptions{
        .sender_queue_size = 1024,
        .max_pending_acks = 64,
        .receiver_queue_size = receiverQueueSize,
        .sender_io_threads = 1,
        .receiver_io_threads = 1,
    };
}

class NetworkSinkTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NetworkSinkTest.log", NES::LogLevel::LOG_DEBUG);
        enable_memcom();
    }

    /// Every test uses its own addresses, because the network services are process-wide singletons.
    void startServices(const uint16_t port, const uint32_t receiverQueueSize)
    {
        receiverAddress = fmt::format("receiver:{}", port);
        senderAddress = fmt::format("sender:{}", port);
        init_receiver_service(rust::String(receiverAddress), rust::String("receiver"), options(receiverQueueSize));
        init_sender_service(rust::String(senderAddress), rust::String("sender"), options(receiverQueueSize));
    }

    std::unique_ptr<NetworkSink> createSink(const std::string& channel)
    {
        const Schema<UnqualifiedUnboundField, Ordered> schema{
            UnqualifiedUnboundField{Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}};
        const std::unordered_map<Identifier, std::string> config{
            {Identifier::parse("DATA_ENDPOINT"), receiverAddress},
            {Identifier::parse("BIND"), senderAddress},
            {Identifier::parse("CHANNEL"), channel}};
        const auto descriptor = SinkDescriptor::createNamed(
            INVALID_SINK_ID, Identifier::parse("network_sink"), Identifier::parse("Network"), schema, Host{"localhost"}, config, {});
        if (!descriptor.has_value())
        {
            ADD_FAILURE() << "Could not create the descriptor of the network sink";
            return nullptr;
        }
        auto [controller, listener] = createBackpressureChannel();
        backpressureListener.emplace(std::move(listener));
        return std::make_unique<NetworkSink>(
            std::move(controller),
            NetworkSinkConfig{
                .dataEndpoint = receiverAddress, .bind = senderAddress, .channel = channel, .senderQueueSize = 0, .maxPendingAcks = 0},
            descriptor.value());
    }

    std::unique_ptr<NetworkSource> createSource(const std::string& channel) const
    {
        auto source
            = std::make_unique<NetworkSource>(NetworkSourceConfig{.channel = channel, .bind = receiverAddress, .receiverQueueSize = 0});
        source->open(bufferManager);
        return source;
    }

    /// A buffer with a single tuple, whose sequence number is also its value.
    TupleBuffer createBuffer(const uint64_t sequenceNumber) const
    {
        auto buffer = bufferManager->getBufferBlocking();
        std::memcpy(buffer.getAvailableMemoryArea<uint8_t>().data(), &sequenceNumber, sizeof(sequenceNumber));
        buffer.setNumberOfTuples(1);
        buffer.setSequenceNumber(SequenceNumber(sequenceNumber));
        buffer.setChunkNumber(ChunkNumber(1));
        buffer.setLastChunk(true);
        buffer.setOriginId(OriginId(1));
        buffer.setWatermark(Timestamp(sequenceNumber));
        return buffer;
    }

    void send(NetworkSink& sink, TestPipelineExecutionContext& pec, const uint64_t first, const uint64_t last) const
    {
        for (auto sequenceNumber = first; sequenceNumber <= last; ++sequenceNumber)
        {
            sink.execute(createBuffer(sequenceNumber), pec);
        }
    }

    /// Executes the tasks the sink repeated, like the task queue would, until it repeats no more tasks.
    static void runRepeatedTasks(NetworkSink& sink, TestPipelineExecutionContext& pec)
    {
        const auto deadline = std::chrono::steady_clock::now() + TIMEOUT;
        while (!pec.repeated.empty())
        {
            ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "The sink keeps repeating its task";
            auto buffer = std::move(pec.repeated.front());
            pec.repeated.pop_front();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if (!buffer)
            {
                /// The sink repeats its stop with an empty buffer.
                sink.stop(pec);
            }
            else
            {
                sink.execute(buffer, pec);
            }
        }
    }

    static void stopSink(NetworkSink& sink, TestPipelineExecutionContext& pec)
    {
        sink.stop(pec);
        runRepeatedTasks(sink, pec);
    }

    ReconfigurationMarker updateMarker(const std::string& oldChannel, const std::string& newChannel) const
    {
        auto marker = ReconfigurationMarker::create();
        marker.addReconfigurationEvent(
            localQueryId,
            UpdateQueryMetadata{
                .networkSinkUpdates = {{.oldChannel = oldChannel, .newChannel = newChannel, .newDataEndpoint = receiverAddress}}});
        return marker;
    }

    const LocalQueryId localQueryId{generateUUID()};
    ReconfigurationState reconfigurationState{localQueryId};
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(64 * 1024 * 1024, 0.5, BufferAlignment{64}, 8192, std::make_shared<NesDefaultMemoryAllocator>());
    TestPipelineExecutionContext pec{reconfigurationState, bufferManager};
    std::string receiverAddress;
    std::string senderAddress;
    std::optional<BackpressureListener> backpressureListener;
};

/// Update: the old receiver gets all data sent before the update, followed by the update marker. Later data goes to the new receiver.
TEST_F(NetworkSinkTest, UpdateSendsTheMarkerToTheOldReceiverAndConnectsToTheNewOne)
{
    startServices(3001, 10);
    auto oldSource = createSource("old");
    auto newSource = createSource("new");
    auto sink = createSink("old");
    sink->start(pec);

    send(*sink, pec, 1, 3);
    const auto marker = updateMarker("old", "new");
    ASSERT_TRUE(reconfigurationState.recordMarker(marker).has_value());
    send(*sink, pec, 4, 5);
    stopSink(*sink, pec);

    const auto old = receiveUntilEndOfStream(*oldSource, *bufferManager);
    EXPECT_EQ(old.sequenceNumbers, range(1, 3));
    ASSERT_TRUE(old.marker.has_value());
    EXPECT_EQ(old.marker->getId(), marker.getId());
    const auto updated = receiveUntilEndOfStream(*newSource, *bufferManager);
    EXPECT_EQ(updated.sequenceNumbers, range(4, 5));
    EXPECT_FALSE(updated.marker.has_value());
    oldSource->close();
    newSource->close();
}

/// Update after the old receiver detached (it was drained before, e.g., because its link was removed): the sink buffers its data and the
/// update redirects the parked channel, so the new receiver gets everything the old one did not take, in order.
TEST_F(NetworkSinkTest, UpdateRedirectsAParkedChannelWithoutLosingData)
{
    constexpr uint64_t NUMBER_OF_BUFFERS = 50;
    startServices(3002, 2);
    auto oldSource = createSource("old");
    auto newSource = createSource("new");
    auto sink = createSink("old");
    sink->start(pec);

    send(*sink, pec, 1, NUMBER_OF_BUFFERS / 2);
    const auto firstReceived = receive(*oldSource, *bufferManager, 3);
    ASSERT_TRUE(oldSource->drain());
    const auto restOfOld = receiveUntilEndOfStream(*oldSource, *bufferManager);
    /// While the receiver is detached, the sink keeps repeating its tasks, so they are run only after the update.
    send(*sink, pec, (NUMBER_OF_BUFFERS / 2) + 1, NUMBER_OF_BUFFERS);

    /// The new receiver has a small queue, so it has to consume while the sink sends.
    auto redirectedFuture = std::async(std::launch::async, [&] { return receiveUntilEndOfStream(*newSource, *bufferManager); });
    ASSERT_TRUE(reconfigurationState.recordMarker(updateMarker("old", "new")).has_value());
    runRepeatedTasks(*sink, pec);
    stopSink(*sink, pec);

    auto received = firstReceived;
    received.insert(received.end(), restOfOld.sequenceNumbers.begin(), restOfOld.sequenceNumbers.end());
    const auto redirected = redirectedFuture.get();
    received.insert(received.end(), redirected.sequenceNumbers.begin(), redirected.sequenceNumbers.end());
    EXPECT_EQ(received, range(1, NUMBER_OF_BUFFERS));
    oldSource->close();
    newSource->close();
}

/// A drained query forwards its marker to receivers that are drained as well.
TEST_F(NetworkSinkTest, DrainForwardsTheMarker)
{
    startServices(3003, 10);
    auto source = createSource("c");
    auto sink = createSink("c");
    sink->start(pec);
    send(*sink, pec, 1, 2);

    auto marker = ReconfigurationMarker::create();
    marker.addReconfigurationEvent(localQueryId, DrainQueryMetadata{.detachedChannels = {"other"}});
    ASSERT_TRUE(reconfigurationState.recordMarker(marker).has_value());
    stopSink(*sink, pec);

    const auto received = receiveUntilEndOfStream(*source, *bufferManager);
    EXPECT_EQ(received.sequenceNumbers, range(1, 2));
    ASSERT_TRUE(received.marker.has_value());
    EXPECT_EQ(received.marker->getId(), marker.getId());
    source->close();
}

/// A drained query detaches from receivers that keep running: the receiver does not end its stream and a new sender can attach to it.
TEST_F(NetworkSinkTest, DrainDetachesFromReceiversThatKeepRunning)
{
    startServices(3004, 10);
    auto source = createSource("c");
    auto sink = createSink("c");
    sink->start(pec);
    send(*sink, pec, 1, 2);

    auto marker = ReconfigurationMarker::create();
    marker.addReconfigurationEvent(localQueryId, UpdateAndDrainQueryMetadata{.detachedChannels = {"c"}, .stateMigrationChannels = {}});
    ASSERT_TRUE(reconfigurationState.recordMarker(marker).has_value());
    stopSink(*sink, pec);
    EXPECT_EQ(receive(*source, *bufferManager, 2), range(1, 2));

    /// The next sender attaches to the same channel and continues the stream.
    ReconfigurationState nextQueryState{LocalQueryId(generateUUID())};
    TestPipelineExecutionContext nextPec{nextQueryState, bufferManager};
    auto nextSink = createSink("c");
    nextSink->start(nextPec);
    send(*nextSink, nextPec, 3, 4);
    stopSink(*nextSink, nextPec);
    const auto received = receiveUntilEndOfStream(*source, *bufferManager);
    EXPECT_EQ(received.sequenceNumbers, range(3, 4));
    EXPECT_FALSE(received.marker.has_value());
    source->close();
}

/// A sink that waits for a new receiver drops its data when its query is stopped, so the query can terminate.
TEST_F(NetworkSinkTest, StoppedQueryEndsWaitingForANewReceiver)
{
    startServices(3005, 2);
    auto source = createSource("c");
    auto sink = createSink("c");
    sink->start(pec);
    send(*sink, pec, 1, 5);
    (void)receive(*source, *bufferManager, 1);
    ASSERT_TRUE(source->drain());
    (void)receiveUntilEndOfStream(*source, *bufferManager);
    send(*sink, pec, 6, 20);
    EXPECT_FALSE(pec.repeated.empty()) << "the sink waits for a new receiver";

    reconfigurationState.markStopped();
    runRepeatedTasks(*sink, pec);
    stopSink(*sink, pec);
    source->close();
}

}
}
