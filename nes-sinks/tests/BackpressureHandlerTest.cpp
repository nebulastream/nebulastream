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
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <gtest/gtest.h>
#include <BackpressureManager.hpp>

namespace NES
{
namespace
{
class BackpressureHandlerTest : public testing::Test
{
protected:
    std::shared_ptr<BufferManager> bufferManager
        = BufferManager::create(4096, 0.0, BufferAlignment{64}, 256, std::make_shared<NesDefaultMemoryAllocator>());

    TupleBuffer makeBuffer(OriginId origin, SequenceNumber sequence, ChunkNumber chunk)
    {
        auto buffer = bufferManager->getBufferBlocking();
        buffer.setOriginId(origin);
        buffer.setSequenceNumber(sequence);
        buffer.setChunkNumber(chunk);
        return buffer;
    }

    static void expectBuffer(const std::optional<TupleBuffer>& actual, const TupleBuffer& expected)
    {
        ASSERT_TRUE(actual.has_value());
        EXPECT_EQ(actual->getAvailableMemoryArea().data(), expected.getAvailableMemoryArea().data());
    }
};

TEST_F(BackpressureHandlerTest, UnrelatedSuccessDoesNotCreateAnotherRetry)
{
    const BackpressureManager backpressure({OriginId{1}}, {});
    auto voter = backpressure.newExternalVoter();
    BackpressureHandler handler;
    handler.start(std::move(voter));
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});
    const auto c = makeBuffer(OriginId{1}, SequenceNumber{3}, ChunkNumber{1});
    const auto d = makeBuffer(OriginId{1}, SequenceNumber{4}, ChunkNumber{1});

    expectBuffer(handler.onFull(a), a);
    EXPECT_FALSE(handler.onSuccess(b));
    EXPECT_FALSE(handler.onFull(c));
    EXPECT_FALSE(handler.onSuccess(b));
    EXPECT_FALSE(handler.onFull(d));
    expectBuffer(handler.onFull(a), a);
    expectBuffer(handler.onSuccess(a), c);
    expectBuffer(handler.onSuccess(c), d);
    EXPECT_FALSE(handler.onSuccess(d));
    EXPECT_TRUE(handler.empty());
}

TEST_F(BackpressureHandlerTest, DequeuedBufferRemainsPendingWhenRejectedAgain)
{
    const BackpressureManager backpressure({OriginId{1}}, {});
    auto voter = backpressure.newExternalVoter();
    BackpressureHandler handler;
    handler.start(std::move(voter));
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});
    const auto c = makeBuffer(OriginId{1}, SequenceNumber{3}, ChunkNumber{1});

    expectBuffer(handler.onFull(a), a);
    EXPECT_FALSE(handler.onFull(b));
    EXPECT_FALSE(handler.onFull(c));
    expectBuffer(handler.onSuccess(a), b);
    expectBuffer(handler.onFull(b), b);
    expectBuffer(handler.onFull(b), b);
    /// A completed already; its success must not clear B's new pending marker.
    EXPECT_FALSE(handler.onSuccess(a));
    expectBuffer(handler.onSuccess(b), c);
    EXPECT_FALSE(handler.onSuccess(c));
    EXPECT_TRUE(handler.empty());
}

TEST_F(BackpressureHandlerTest, PendingIdentityIncludesOriginAndChunk)
{
    const BackpressureManager backpressure({OriginId{1}}, {});
    auto voter = backpressure.newExternalVoter();
    BackpressureHandler handler;
    handler.start(std::move(voter));
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto otherOrigin = makeBuffer(OriginId{2}, SequenceNumber{1}, ChunkNumber{1});
    const auto otherChunk = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{2});

    expectBuffer(handler.onFull(a), a);
    /// A successful buffer with a different origin or chunk must leave A pending.
    EXPECT_FALSE(handler.onSuccess(otherOrigin));
    EXPECT_FALSE(handler.onSuccess(otherChunk));
    EXPECT_FALSE(handler.onFull(otherOrigin));
    EXPECT_FALSE(handler.onFull(otherChunk));
    expectBuffer(handler.onSuccess(a), otherOrigin);
    expectBuffer(handler.onFull(otherOrigin), otherOrigin);
    expectBuffer(handler.onSuccess(otherOrigin), otherChunk);
    EXPECT_FALSE(handler.onSuccess(otherChunk));
    EXPECT_TRUE(handler.empty());
}

TEST_F(BackpressureHandlerTest, UnrelatedSuccessDoesNotReleasePendingBackpressure)
{
    const BackpressureManager backpressure({OriginId{1}}, {});
    auto voter = backpressure.newExternalVoter();
    const auto listener = backpressure.getListener(OriginId{1});
    BackpressureHandler handler(1, 0);
    handler.start(std::move(voter));
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});

    expectBuffer(handler.onFull(a), a);
    EXPECT_TRUE(listener.isBlocked());
    EXPECT_FALSE(handler.onSuccess(b));
    EXPECT_TRUE(listener.isBlocked());
    expectBuffer(handler.onFull(a), a);
    EXPECT_FALSE(handler.onSuccess(a));
    EXPECT_FALSE(listener.isBlocked());
    /// A fresh failure must start a new retry chain after the old one drained.
    expectBuffer(handler.onFull(b), b);
    EXPECT_FALSE(handler.onSuccess(b));
    EXPECT_FALSE(listener.isBlocked());
}

TEST_F(BackpressureHandlerTest, BackpressureRemainsUntilBacklogDrains)
{
    const BackpressureManager backpressure({OriginId{1}}, {});
    auto voter = backpressure.newExternalVoter();
    const auto listener = backpressure.getListener(OriginId{1});
    BackpressureHandler handler(2, 0);
    handler.start(std::move(voter));
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});
    const auto c = makeBuffer(OriginId{1}, SequenceNumber{3}, ChunkNumber{1});

    expectBuffer(handler.onFull(a), a);
    EXPECT_FALSE(listener.isBlocked());
    EXPECT_FALSE(handler.onFull(b));
    EXPECT_FALSE(listener.isBlocked());
    EXPECT_FALSE(handler.onFull(c));
    EXPECT_TRUE(listener.isBlocked());
    expectBuffer(handler.onSuccess(a), b);
    EXPECT_TRUE(listener.isBlocked());
    expectBuffer(handler.onSuccess(b), c);
    EXPECT_FALSE(handler.onSuccess(c));
    EXPECT_FALSE(listener.isBlocked());
    EXPECT_TRUE(handler.empty());
}
}
}
