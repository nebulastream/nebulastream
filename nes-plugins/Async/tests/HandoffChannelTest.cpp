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

#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/UUID.hpp>
#include <gtest/gtest.h>
#include <HandoffChannel.hpp>

namespace NES
{
namespace
{

/// The registry is not visible from outside, so the tests tell a removed channel from one left behind by what a new
/// lease on the same id finds: a removed channel is created anew, open and empty.
class HandoffChannelTest : public testing::Test
{
protected:
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(4096, 0.0, BufferAlignment{64}, 8, std::make_shared<NesDefaultMemoryAllocator>());
    /// Every buffer a test takes is back in the pool once the channel dropped or handed it on.
    size_t allBuffers = bufferManager->getNumberOfAvailableBuffers();
    std::string id = UUIDToString(generateUUID());

    [[nodiscard]] bool registeredChannelIsFresh() const
    {
        const HandoffChannel::Lease sink{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SINK};
        return !sink->exhausted() && sink->tryPop() == std::nullopt;
    }
};

TEST_F(HandoffChannelTest, TakesNoBufferBeforeTheSourceAttached)
{
    HandoffChannel::Lease sink{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SINK};
    EXPECT_EQ(sink->tryPush(bufferManager->getBufferBlocking()), HandoffChannel::PushResult::FULL);

    const HandoffChannel::Lease source{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SOURCE};
    EXPECT_EQ(sink->tryPush(bufferManager->getBufferBlocking()), HandoffChannel::PushResult::PUSHED);
}

TEST_F(HandoffChannelTest, HandsOffAndEndsTheStreamWhenTheSinkClosesFirst)
{
    std::optional<HandoffChannel::Lease> sink{std::in_place, id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SINK};
    std::optional<HandoffChannel::Lease> source{std::in_place, id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SOURCE};
    ASSERT_EQ((*sink)->tryPush(bufferManager->getBufferBlocking()), HandoffChannel::PushResult::PUSHED);
    (*sink)->close();
    sink.reset();

    EXPECT_TRUE((*source)->pop(std::stop_token{}).has_value());
    EXPECT_TRUE((*source)->exhausted());
    source.reset();
    EXPECT_TRUE(registeredChannelIsFresh());
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
}

/// The source's construction fails after it took its lease, e.g. on an invalid function configuration.
TEST_F(HandoffChannelTest, SinkLearnsThatAFailedSourceIsGone)
{
    std::optional<HandoffChannel::Lease> sink{std::in_place, id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SINK};
    {
        const HandoffChannel::Lease source{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SOURCE};
        ASSERT_EQ((*sink)->tryPush(bufferManager->getBufferBlocking()), HandoffChannel::PushResult::PUSHED);
    }
    EXPECT_EQ((*sink)->tryPush(bufferManager->getBufferBlocking()), HandoffChannel::PushResult::CONSUMER_GONE);
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
    sink.reset();
    EXPECT_TRUE(registeredChannelIsFresh());
}

/// The source half never got as far as its lease, e.g. because it failed to lower, and the teardown destroys the sink
/// without a graceful stop.
TEST_F(HandoffChannelTest, SinkTornDownWithoutASourceRemovesTheChannel)
{
    {
        const HandoffChannel::Lease sink{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SINK};
        EXPECT_EQ(sink->tryPush(bufferManager->getBufferBlocking()), HandoffChannel::PushResult::FULL);
    }
    EXPECT_TRUE(registeredChannelIsFresh());
    EXPECT_EQ(bufferManager->getNumberOfAvailableBuffers(), allBuffers);
}

/// The source failed and went before the sink was even created: the sink gets a channel of its own, which never takes a
/// buffer, and the teardown removes it with the sink.
TEST_F(HandoffChannelTest, SinkAfterAFailedSourceIsHeldBackUntilTornDown)
{
    {
        const HandoffChannel::Lease source{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SOURCE};
    }
    {
        const HandoffChannel::Lease sink{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SINK};
        EXPECT_EQ(sink->tryPush(bufferManager->getBufferBlocking()), HandoffChannel::PushResult::FULL);
    }
    EXPECT_TRUE(registeredChannelIsFresh());
}

/// A producer without data stops gracefully before the source attached: the source that comes later ends its stream.
TEST_F(HandoffChannelTest, SourceAfterAGracefullyClosedSinkEndsItsStream)
{
    {
        const HandoffChannel::Lease sink{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SINK};
        sink->close();
    }
    {
        const HandoffChannel::Lease source{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SOURCE};
        EXPECT_EQ(source->pop(std::stop_token{}), std::nullopt);
        EXPECT_TRUE(source->exhausted());
    }
    EXPECT_TRUE(registeredChannelIsFresh());
}

TEST_F(HandoffChannelTest, ReleaseIsIdempotent)
{
    HandoffChannel::Lease source{id, HandoffChannel::DEFAULT_CAPACITY, HandoffChannel::Role::SOURCE};
    source.release();
    source.release();
    EXPECT_TRUE(registeredChannelIsFresh());
}

}
}
