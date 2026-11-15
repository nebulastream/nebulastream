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
#include <future>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <Async/HandoffChannel.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Reflection.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
using PushResult = HandoffChannel::PushResult;
static_assert(!std::is_copy_constructible_v<HandoffChannel>);
static_assert(std::is_nothrow_move_constructible_v<HandoffChannel>);
static_assert(!std::is_copy_constructible_v<HandoffChannel::Sender>);
static_assert(!std::is_copy_constructible_v<HandoffChannel::Receiver>);
static_assert(std::is_nothrow_move_constructible_v<HandoffChannel::Sender>);
static_assert(std::is_nothrow_move_assignable_v<HandoffChannel::Receiver>);

class HandoffChannelTest : public testing::Test
{
protected:
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(4096, 0.0, BufferAlignment{64}, 8, std::make_shared<NesDefaultMemoryAllocator>());
    size_t allBuffers = bufferManager->getNumberOfAvailableBuffers();
};

TEST_F(HandoffChannelTest, PositiveCapacityAndBoundedFifo)
{
    EXPECT_THROW(HandoffChannel::create(0), Exception);
    EXPECT_THROW(HandoffChannel{0}, Exception);
    auto [sender, receiver] = HandoffChannel::create(1);
    EXPECT_FALSE(receiver.exhausted());
    EXPECT_FALSE(receiver.tryPop());
    auto buffer = bufferManager->getBufferBlocking();
    EXPECT_EQ(sender.tryPush(buffer), PushResult::PUSHED);
    EXPECT_EQ(sender.tryPush(buffer), PushResult::FULL);
    auto received = receiver.tryPop();
    ASSERT_TRUE(received);
    EXPECT_EQ(received->getAvailableMemoryArea().data(), buffer.getAvailableMemoryArea().data());
    EXPECT_EQ(sender.tryPush(buffer), PushResult::PUSHED);
    sender.close();
    sender.close();
    EXPECT_THROW(static_cast<void>(sender.tryPush(buffer)), Exception);
    EXPECT_FALSE(receiver.exhausted());
    EXPECT_TRUE(receiver.pop({}));
    EXPECT_TRUE(receiver.exhausted());
    EXPECT_FALSE(receiver.pop({}));
}

TEST_F(HandoffChannelTest, EitherEndpointCanBeClaimedFirst)
{
    for (bool senderFirst : {false, true})
    {
        HandoffChannel channels{1};
        std::optional<HandoffChannel::Sender> sender;
        std::optional<HandoffChannel::Receiver> receiver;
        if (senderFirst)
        {
            sender.emplace(channels.takeSender());
            EXPECT_EQ(sender->tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
            sender->close();
            sender.reset();
            receiver.emplace(channels.takeReceiver());
        }
        else
        {
            receiver.emplace(channels.takeReceiver());
            sender.emplace(channels.takeSender());
            EXPECT_EQ(sender->tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
            sender->close();
            sender.reset();
        }
        EXPECT_NO_THROW(channels.validate());
        EXPECT_TRUE(receiver->pop({}));
        EXPECT_TRUE(receiver->exhausted());
    }
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
}

TEST_F(HandoffChannelTest, AbortDiscardsQueuedDataAndNeverLooksLikeEos)
{
    auto [sender, receiver] = HandoffChannel::create(1);
    std::optional<HandoffChannel::Sender> owner{std::move(sender)};
    EXPECT_EQ(owner->tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    owner.reset();
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
    EXPECT_THROW(receiver.pop({}), Exception);
    EXPECT_THROW(receiver.tryPop(), Exception);
    EXPECT_THROW(static_cast<void>(receiver.exhausted()), Exception);
    receiver.close();
    EXPECT_THROW(static_cast<void>(receiver.exhausted()), Exception);
}

TEST_F(HandoffChannelTest, AbortWakesWaitingReceiver)
{
    auto [sender, receiver] = HandoffChannel::create(1);
    std::optional<HandoffChannel::Sender> owner{std::move(sender)};
    std::promise<void> started;
    std::promise<bool> result;
    auto future = result.get_future();
    std::jthread waiter(
        [&](std::stop_token token)
        {
            started.set_value();
            try
            {
                receiver.pop(token);
                result.set_value(false);
            }
            catch (const Exception&)
            {
                result.set_value(true);
            }
        });
    started.get_future().wait();
    owner.reset();
    EXPECT_EQ(future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    waiter.request_stop();
    waiter.join();
    EXPECT_TRUE(future.get());
}

TEST_F(HandoffChannelTest, StopRequestInterruptsPopWithoutClosingChannel)
{
    auto [sender, receiver] = HandoffChannel::create(1);
    std::stop_source stop;
    stop.request_stop();
    EXPECT_FALSE(receiver.pop(stop.get_token()));
    EXPECT_FALSE(receiver.exhausted());
    EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
}

TEST_F(HandoffChannelTest, StoppedReceiverDoesNotReportProducerAbortAsFailure)
{
    auto [sender, receiver] = HandoffChannel::create(1);
    std::optional<HandoffChannel::Sender> owner{std::move(sender)};
    std::stop_source stop;
    stop.request_stop();
    owner.reset();
    EXPECT_FALSE(receiver.pop(stop.get_token()));
    EXPECT_THROW(receiver.pop({}), Exception);
}

TEST_F(HandoffChannelTest, ReceiverCloseDropsBuffersAndClearsListener)
{
    auto [sender, receiver] = HandoffChannel::create(1);
    size_t changes = 0;
    receiver.setListener([&] { ++changes; });
    EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    EXPECT_EQ(changes, 1);
    receiver.close();
    receiver.close();
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
    EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::CONSUMER_GONE);
    sender.close();
    EXPECT_EQ(changes, 1);
}

TEST_F(HandoffChannelTest, ReceiverDestructionRejectsPushes)
{
    auto [sender, receiver] = HandoffChannel::create(1);
    {
        auto owner = std::move(receiver);
        EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    }
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
    EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::CONSUMER_GONE);
}

TEST_F(HandoffChannelTest, ListenerObservesNormalCloseAndAbort)
{
    auto [sender, receiver] = HandoffChannel::create(1);
    size_t changes = 0;
    receiver.setListener([&] { ++changes; });
    sender.close();
    sender.close();
    EXPECT_EQ(changes, 1);
    receiver.setListener({});

    auto [abortingSender, abortedReceiver] = HandoffChannel::create(1);
    abortedReceiver.setListener([&] { ++changes; });
    {
        auto owner = std::move(abortingSender);
    }
    EXPECT_EQ(changes, 2);
    abortedReceiver.close();
}

TEST_F(HandoffChannelTest, IncompleteClaimsValidationAndCleanup)
{
    std::optional<HandoffChannel::Sender> sender;
    {
        HandoffChannel channels{1};
        EXPECT_THROW(channels.validate(), Exception);
        sender.emplace(channels.takeSender());
        EXPECT_EQ(sender->tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
        EXPECT_THROW(channels.validate(), Exception);
    }
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
    EXPECT_EQ(sender->tryPush(bufferManager->getBufferBlocking()), PushResult::CONSUMER_GONE);

    std::optional<HandoffChannel::Receiver> receiver;
    {
        HandoffChannel channels{1};
        receiver.emplace(channels.takeReceiver());
        EXPECT_THROW(channels.validate(), Exception);
    }
    EXPECT_THROW(static_cast<void>(receiver->exhausted()), Exception);
}

TEST_F(HandoffChannelTest, ClaimsCannotBeRepeatedEvenAfterEndpointRelease)
{
    HandoffChannel channels{1};
    {
        auto sender = channels.takeSender();
        EXPECT_THROW(channels.takeSender(), Exception);
    }
    EXPECT_THROW(channels.takeSender(), Exception);
    {
        auto receiver = channels.takeReceiver();
        EXPECT_THROW(receiver.tryPop(), Exception);
    }
    EXPECT_THROW(channels.takeReceiver(), Exception);
    EXPECT_NO_THROW(channels.validate());

    HandoffChannel reverse{2};
    auto receiver = reverse.takeReceiver();
    EXPECT_THROW(reverse.takeReceiver(), Exception);
    EXPECT_THROW(reverse.validate(), Exception);
    auto sender = reverse.takeSender();
    EXPECT_NO_THROW(reverse.validate());
}

TEST_F(HandoffChannelTest, MoveAssignmentReleasesPreviousEndpoints)
{
    auto [firstSender, firstReceiver] = HandoffChannel::create(1);
    auto [secondSender, secondReceiver] = HandoffChannel::create(1);
    firstSender = std::move(secondSender);
    EXPECT_THROW(static_cast<void>(firstReceiver.exhausted()), Exception);
    EXPECT_EQ(firstSender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    firstReceiver = std::move(secondReceiver);
    EXPECT_TRUE(firstReceiver.tryPop());

    auto [thirdSender, thirdReceiver] = HandoffChannel::create(1);
    EXPECT_EQ(thirdSender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    thirdReceiver = std::move(firstReceiver);
    EXPECT_EQ(thirdSender.tryPush(bufferManager->getBufferBlocking()), PushResult::CONSUMER_GONE);
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
    firstSender.close();
    EXPECT_TRUE(thirdReceiver.exhausted());
}

TEST_F(HandoffChannelTest, DistinctChannelObjectsAreIsolated)
{
    HandoffChannel first{1};
    HandoffChannel second{1};
    HandoffChannel third{1};
    auto sender = first.takeSender();
    auto receiver = first.takeReceiver();
    auto otherSender = second.takeSender();
    auto otherReceiver = second.takeReceiver();
    auto distinctReceiver = third.takeReceiver();
    EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    EXPECT_FALSE(otherReceiver.tryPop());
    EXPECT_FALSE(distinctReceiver.tryPop());
    EXPECT_TRUE(receiver.tryPop());
    receiver.close();
    EXPECT_EQ(otherSender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    EXPECT_TRUE(otherReceiver.tryPop());
}

TEST_F(HandoffChannelTest, SharedConfigurationReflectionPreservesIdentityAndCreatesFreshQueue)
{
    auto channel = std::make_shared<HandoffChannel>(2);
    const ReflectionContext writer;
    const auto sourceConfig = writer.reflect(channel);
    auto sender = channel->takeSender();
    auto receiver = channel->takeReceiver();
    EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);

    /// Claims and queued buffers must not change the serialized definition.
    const auto sinkConfig = writer.reflect(channel);
    const ReflectionContext reader;
    const auto sourceChannel = reader.unreflect<std::shared_ptr<HandoffChannel>>(sourceConfig);
    const auto sinkChannel = reader.unreflect<std::shared_ptr<HandoffChannel>>(sinkConfig);
    EXPECT_EQ(sourceChannel, sinkChannel);
    EXPECT_NE(sourceChannel, channel);
    EXPECT_EQ(sourceChannel->getCapacity(), 2);
    EXPECT_THROW(reader.validateSharedObjects(), Exception);
    auto freshReceiver = sourceChannel->takeReceiver();
    EXPECT_FALSE(freshReceiver.tryPop());
    EXPECT_THROW(reader.validateSharedObjects(), Exception);
    auto freshSender = sinkChannel->takeSender();
    EXPECT_NO_THROW(reader.validateSharedObjects());
    EXPECT_EQ(freshSender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    EXPECT_TRUE(freshReceiver.tryPop());
    EXPECT_TRUE(receiver.tryPop());

    const auto separate = ReflectionContext{}.unreflect<std::shared_ptr<HandoffChannel>>(sourceConfig);
    EXPECT_NE(separate, sourceChannel);
    EXPECT_THROW(separate->validate(), Exception);
}

TEST_F(HandoffChannelTest, MovingChannelPreservesQueueAndClaims)
{
    HandoffChannel original{1};
    auto sender = original.takeSender();
    EXPECT_EQ(sender.tryPush(bufferManager->getBufferBlocking()), PushResult::PUSHED);
    HandoffChannel moved{std::move(original)};
    EXPECT_EQ(moved.getCapacity(), 1);
    EXPECT_THROW(moved.takeSender(), Exception);
    EXPECT_THROW(moved.validate(), Exception);
    auto receiver = moved.takeReceiver();
    EXPECT_TRUE(receiver.tryPop());
    EXPECT_NO_THROW(moved.validate());
}

}
}
