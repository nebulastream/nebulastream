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
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include <Async/HandoffChannel.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 1024;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 64;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.1;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

std::shared_ptr<BufferManager> makeBufferManager()
{
    return BufferManager::create(
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, POOLED_BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
}

/// A buffer tagged with `marker` in its first byte, so the test can tell buffers apart.
TupleBuffer taggedBuffer(BufferManager& bufferManager, const uint8_t marker)
{
    auto buffer = bufferManager.getBufferBlocking();
    buffer.getAvailableMemoryArea<uint8_t>()[0] = marker;
    buffer.setNumberOfTuples(marker);
    return buffer;
}

uint8_t markerOf(const TupleBuffer& buffer)
{
    return buffer.getAvailableMemoryArea<uint8_t>()[0];
}
}

class HandoffChannelTest : public ::testing::Test
{
};

TEST_F(HandoffChannelTest, HandsBuffersOverInOrder)
{
    const auto bufferManager = makeBufferManager();
    HandoffChannel channel{8};
    const std::stop_source stopSource;

    for (uint8_t marker = 1; marker <= 3; ++marker)
    {
        ASSERT_TRUE(channel.tryPush(taggedBuffer(*bufferManager, marker)));
    }
    EXPECT_EQ(channel.size(), 3U);

    for (uint8_t expected = 1; expected <= 3; ++expected)
    {
        const auto buffer = channel.popBlocking(stopSource.get_token());
        ASSERT_TRUE(buffer.has_value());
        EXPECT_EQ(markerOf(*buffer), expected);
        EXPECT_EQ(buffer->getNumberOfTuples(), expected);
    }
    EXPECT_EQ(channel.size(), 0U);
}

TEST_F(HandoffChannelTest, RejectsPushWhenFull)
{
    const auto bufferManager = makeBufferManager();
    HandoffChannel channel{2};

    EXPECT_TRUE(channel.tryPush(taggedBuffer(*bufferManager, 1)));
    EXPECT_TRUE(channel.tryPush(taggedBuffer(*bufferManager, 2)));
    /// Full: the producer is expected to stash this buffer and retry.
    EXPECT_FALSE(channel.tryPush(taggedBuffer(*bufferManager, 3)));
    EXPECT_EQ(channel.size(), 2U);
}

TEST_F(HandoffChannelTest, DrainsBeforeReportingTheEnd)
{
    const auto bufferManager = makeBufferManager();
    HandoffChannel channel{8};
    const std::stop_source stopSource;

    ASSERT_TRUE(channel.tryPush(taggedBuffer(*bufferManager, 1)));
    ASSERT_TRUE(channel.tryPush(taggedBuffer(*bufferManager, 2)));
    channel.close();
    EXPECT_TRUE(channel.isClosed());

    /// Everything queued before the close is still handed out.
    const auto first = channel.popBlocking(stopSource.get_token());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(markerOf(*first), 1);
    const auto second = channel.popBlocking(stopSource.get_token());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(markerOf(*second), 2);

    /// Only now is it the end of the stream.
    EXPECT_FALSE(channel.popBlocking(stopSource.get_token()).has_value());
}

TEST_F(HandoffChannelTest, ClosedAndEmptyReturnsImmediately)
{
    HandoffChannel channel{8};
    const std::stop_source stopSource;
    channel.close();
    EXPECT_FALSE(channel.popBlocking(stopSource.get_token()).has_value());
}

TEST_F(HandoffChannelTest, RejectsPushAfterClose)
{
    const auto bufferManager = makeBufferManager();
    HandoffChannel channel{8};
    channel.close();
    EXPECT_FALSE(channel.tryPush(taggedBuffer(*bufferManager, 1)));
}

TEST_F(HandoffChannelTest, WaitingConsumerIsWokenByAPush)
{
    const auto bufferManager = makeBufferManager();
    HandoffChannel channel{8};
    const std::stop_source stopSource;

    std::optional<TupleBuffer> received;
    std::jthread consumer{[&] { received = channel.popBlocking(stopSource.get_token()); }};

    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    ASSERT_TRUE(channel.tryPush(taggedBuffer(*bufferManager, 42)));
    consumer.join();

    ASSERT_TRUE(received.has_value());
    EXPECT_EQ(markerOf(*received), 42);
}

TEST_F(HandoffChannelTest, WaitingConsumerIsWokenByAClose)
{
    HandoffChannel channel{8};
    const std::stop_source stopSource;

    std::optional<TupleBuffer> received;
    std::jthread consumer{[&] { received = channel.popBlocking(stopSource.get_token()); }};

    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    channel.close();
    consumer.join();

    EXPECT_FALSE(received.has_value());
}

TEST_F(HandoffChannelTest, StopTokenUnblocksAWaitingConsumer)
{
    HandoffChannel channel{8};
    std::stop_source stopSource;

    std::optional<TupleBuffer> received;
    bool returned = false;
    std::jthread consumer{[&]
                          {
                              received = channel.popBlocking(stopSource.get_token());
                              returned = true;
                          }};

    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    /// A requested stop must end the wait even though the channel was never closed.
    stopSource.request_stop();
    consumer.join();

    EXPECT_TRUE(returned);
    EXPECT_FALSE(received.has_value());
    EXPECT_FALSE(channel.isClosed());
}

TEST_F(HandoffChannelTest, SurvivesConcurrentProducerAndConsumer)
{
    const auto bufferManager = makeBufferManager();
    HandoffChannel channel{4};
    const std::stop_source stopSource;
    constexpr size_t bufferCount = 200;

    std::vector<uint8_t> receivedMarkers;
    std::jthread consumer{[&]
                          {
                              while (auto buffer = channel.popBlocking(stopSource.get_token()))
                              {
                                  receivedMarkers.push_back(markerOf(*buffer));
                              }
                          }};

    for (size_t i = 0; i < bufferCount; ++i)
    {
        auto buffer = taggedBuffer(*bufferManager, static_cast<uint8_t>(i % 256));
        /// Retry on a full channel, exactly as the producer sink will.
        while (!channel.tryPush(buffer))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }
    channel.close();
    consumer.join();

    ASSERT_EQ(receivedMarkers.size(), bufferCount);
    for (size_t i = 0; i < bufferCount; ++i)
    {
        EXPECT_EQ(receivedMarkers[i], static_cast<uint8_t>(i % 256)) << "at position " << i;
    }
}

TEST_F(HandoffChannelTest, ReleasesHeldBuffersWhenDestroyed)
{
    const auto bufferManager = makeBufferManager();
    const auto availableBefore = bufferManager->getNumberOfAvailableBuffers();

    {
        HandoffChannel channel{8};
        for (uint8_t marker = 1; marker <= 4; ++marker)
        {
            ASSERT_TRUE(channel.tryPush(taggedBuffer(*bufferManager, marker)));
        }
        EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), availableBefore - 4);
    }

    /// Buffers parked in a channel must go back to the pool, otherwise BufferManager's
    /// leak check fails at shutdown.
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), availableBefore);
}

TEST_F(HandoffChannelTest, RegistryHandsBothHalvesTheSameChannel)
{
    const std::string channelId = "registry-shared";
    const auto producerSide = HandoffChannelRegistry::getOrCreate(channelId, 8);
    const auto consumerSide = HandoffChannelRegistry::getOrCreate(channelId, 8);

    EXPECT_EQ(producerSide.get(), consumerSide.get());
    EXPECT_EQ(HandoffChannelRegistry::find(channelId).get(), producerSide.get());

    const auto other = HandoffChannelRegistry::getOrCreate("registry-other", 8);
    EXPECT_NE(other.get(), producerSide.get());

    HandoffChannelRegistry::drop(channelId);
    HandoffChannelRegistry::drop("registry-other");
}

TEST_F(HandoffChannelTest, RegistryKeepsCapacityOfTheCreatingCall)
{
    const std::string channelId = "registry-capacity";
    const auto first = HandoffChannelRegistry::getOrCreate(channelId, 3);
    const auto second = HandoffChannelRegistry::getOrCreate(channelId, 99);

    EXPECT_EQ(second->capacity(), 3U);
    HandoffChannelRegistry::drop(channelId);
}

TEST_F(HandoffChannelTest, RegistryForgetsChannelsNobodyHolds)
{
    const std::string channelId = "registry-expiry";
    {
        const auto channel = HandoffChannelRegistry::getOrCreate(channelId, 8);
        EXPECT_NE(HandoffChannelRegistry::find(channelId), nullptr);
    }
    /// Both halves gone: the entry must not keep the channel — or its buffers — alive.
    EXPECT_EQ(HandoffChannelRegistry::find(channelId), nullptr);
}

}
