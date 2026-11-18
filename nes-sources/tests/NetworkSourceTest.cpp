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

/// Tests the drain behaviour of the NetworkSource against the network services, which run over the in-memory transport.

#include <Sources/NetworkSource.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <nes-network-bindings/lib.h>
#include <rust/cxx.h>

namespace NES
{
namespace
{
constexpr auto TIMEOUT = std::chrono::seconds(10);

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

class NetworkSourceTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NetworkSourceTest.log", NES::LogLevel::LOG_DEBUG);
        enable_memcom();
    }

    /// Every test uses its own addresses, because the network services are process-wide singletons.
    static void startServices(const std::string& receiverAddress, const std::string& senderAddress, const uint32_t receiverQueueSize)
    {
        init_receiver_service(rust::String(receiverAddress), rust::String("receiver"), options(receiverQueueSize));
        init_sender_service(rust::String(senderAddress), rust::String("sender"), options(receiverQueueSize));
    }

    static rust::Box<SenderDataChannel>
    registerSender(const std::string& senderAddress, const std::string& receiverAddress, const std::string& channel)
    {
        const auto service = sender_instance(rust::String(senderAddress));
        return register_sender_channel(*service, rust::String(receiverAddress), rust::String(channel), options(0));
    }

    /// Sends a buffer whose sequence number is also its payload.
    static void send(const SenderDataChannel& channel, const uint64_t sequenceNumber)
    {
        const SerializedTupleBufferHeader header{
            .sequence_number = sequenceNumber,
            .origin_id = 1,
            .chunk_number = 1,
            .number_of_tuples = 1,
            .watermark = sequenceNumber,
            .last_chunk = true};
        const auto* payload = reinterpret_cast<const uint8_t*>(&sequenceNumber); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        const std::vector<rust::Slice<const uint8_t>> children;
        ASSERT_EQ(
            send_buffer(
                channel,
                header,
                rust::Slice<const uint8_t>(payload, sizeof(sequenceNumber)),
                rust::Slice<const rust::Slice<const uint8_t>>(children.data(), children.size())),
            SendResult::Ok);
    }

    static void waitForFlushResult(const SenderDataChannel& channel, const FlushResult expected)
    {
        const auto deadline = std::chrono::steady_clock::now() + TIMEOUT;
        while (flush_sender_channel(channel) != expected)
        {
            ASSERT_LT(std::chrono::steady_clock::now(), deadline) << "Timed out waiting for the flush result";
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(64 * 1024 * 1024, 0.5, BufferAlignment{64}, 8192, std::make_shared<NesDefaultMemoryAllocator>());
};

/// The sequence numbers of the buffers the source delivers until its stream ends, and how it ended.
struct Received
{
    std::vector<uint64_t> sequenceNumbers;
    bool endedWithMarker = false;
    std::string marker;
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
            received.endedWithMarker = true;
            received.marker = result.getMarker();
            return received;
        }
        if (result.isEoS())
        {
            return received;
        }
        received.sequenceNumbers.push_back(buffer.getSequenceNumber().getRawValue());
    }
}

TEST_F(NetworkSourceTest, MarkerFromSenderEndsTheStream)
{
    startServices("receiver:2001", "sender:2001", 10);
    NetworkSource source(NetworkSourceConfig{.channel = "c", .bind = "receiver:2001", .receiverQueueSize = 0});
    source.open(bufferManager);

    auto sender = registerSender("sender:2001", "receiver:2001", "c");
    for (uint64_t i = 1; i <= 3; ++i)
    {
        send(*sender, i);
    }
    waitForFlushResult(*sender, FlushResult::Flushed);
    const std::string marker = "serialized marker";
    close_sender_channel_with_marker(
        std::move(sender), rust::Slice<const uint8_t>(reinterpret_cast<const uint8_t*>(marker.data()), marker.size()));

    const auto received = receiveUntilEndOfStream(source, *bufferManager);
    EXPECT_EQ(received.sequenceNumbers, (std::vector<uint64_t>{1, 2, 3}));
    EXPECT_TRUE(received.endedWithMarker);
    EXPECT_EQ(received.marker, marker);
    source.close();
}

/// A drained network source delivers the data it has already received, then its stream ends. The sender keeps everything else and can be
/// redirected to another source, so no buffer is lost or duplicated.
TEST_F(NetworkSourceTest, DrainDetachesAndKeepsUnreceivedDataAtTheSender)
{
    constexpr uint64_t NUMBER_OF_BUFFERS = 100;
    startServices("receiver:2002", "sender:2002", 2);
    NetworkSource oldSource(NetworkSourceConfig{.channel = "old", .bind = "receiver:2002", .receiverQueueSize = 0});
    oldSource.open(bufferManager);
    auto sender = registerSender("sender:2002", "receiver:2002", "old");
    for (uint64_t i = 1; i <= NUMBER_OF_BUFFERS; ++i)
    {
        send(*sender, i);
    }

    /// Receive a few buffers, then drain
    std::vector<uint64_t> sequenceNumbers;
    const std::stop_source neverStopped;
    for (size_t i = 0; i < 5; ++i)
    {
        auto buffer = bufferManager->getBufferBlocking();
        ASSERT_FALSE(oldSource.fillTupleBuffer(buffer, neverStopped.get_token()).isEoS());
        sequenceNumbers.push_back(buffer.getSequenceNumber().getRawValue());
    }
    EXPECT_TRUE(oldSource.drain());
    const auto remainder = receiveUntilEndOfStream(oldSource, *bufferManager);
    EXPECT_FALSE(remainder.endedWithMarker);
    sequenceNumbers.insert(sequenceNumbers.end(), remainder.sequenceNumbers.begin(), remainder.sequenceNumbers.end());
    oldSource.close();
    ASSERT_LT(sequenceNumbers.size(), NUMBER_OF_BUFFERS);

    /// The sender keeps the rest until it is redirected
    waitForFlushResult(*sender, FlushResult::Detached);
    NetworkSource newSource(NetworkSourceConfig{.channel = "new", .bind = "receiver:2002", .receiverQueueSize = 0});
    newSource.open(bufferManager);
    redirect_sender_channel(*sender, rust::String("receiver:2002"), rust::String("new"));
    /// The new source has to receive while the sender flushes, as its small queue applies backpressure.
    Received rest;
    {
        const std::jthread receiver([&] { rest = receiveUntilEndOfStream(newSource, *bufferManager); });
        waitForFlushResult(*sender, FlushResult::Flushed);
        close_sender_channel(std::move(sender));
    }
    sequenceNumbers.insert(sequenceNumbers.end(), rest.sequenceNumbers.begin(), rest.sequenceNumbers.end());
    newSource.close();

    std::vector<uint64_t> expected(NUMBER_OF_BUFFERS);
    std::iota(expected.begin(), expected.end(), 1);
    EXPECT_EQ(sequenceNumbers, expected);
}

/// A drain which arrives before the source has been opened detaches the channel as soon as it is opened.
TEST_F(NetworkSourceTest, DrainBeforeOpenEndsTheStreamRightAway)
{
    startServices("receiver:2003", "sender:2003", 10);
    NetworkSource source(NetworkSourceConfig{.channel = "c", .bind = "receiver:2003", .receiverQueueSize = 0});
    EXPECT_TRUE(source.drain());
    source.open(bufferManager);

    const auto received = receiveUntilEndOfStream(source, *bufferManager);
    EXPECT_TRUE(received.sequenceNumbers.empty());
    EXPECT_FALSE(received.endedWithMarker);
    source.close();
}

}
}
