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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <gtest/gtest.h>
#include <BackpressureChannel.hpp>

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
    auto [controller, listener] = createBackpressureChannel();
    BackpressureHandler handler;
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});
    const auto c = makeBuffer(OriginId{1}, SequenceNumber{3}, ChunkNumber{1});
    const auto d = makeBuffer(OriginId{1}, SequenceNumber{4}, ChunkNumber{1});

    expectBuffer(handler.onFull(a, controller), a);
    EXPECT_FALSE(handler.onSuccess(b, controller));
    EXPECT_FALSE(handler.onFull(c, controller));
    EXPECT_FALSE(handler.onSuccess(b, controller));
    EXPECT_FALSE(handler.onFull(d, controller));
    expectBuffer(handler.onFull(a, controller), a);
    expectBuffer(handler.onSuccess(a, controller), c);
    expectBuffer(handler.onSuccess(c, controller), d);
    EXPECT_FALSE(handler.onSuccess(d, controller));
    EXPECT_TRUE(handler.empty());
}

TEST_F(BackpressureHandlerTest, DequeuedBufferRemainsPendingWhenRejectedAgain)
{
    auto [controller, listener] = createBackpressureChannel();
    BackpressureHandler handler;
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});
    const auto c = makeBuffer(OriginId{1}, SequenceNumber{3}, ChunkNumber{1});

    expectBuffer(handler.onFull(a, controller), a);
    EXPECT_FALSE(handler.onFull(b, controller));
    EXPECT_FALSE(handler.onFull(c, controller));
    expectBuffer(handler.onSuccess(a, controller), b);
    expectBuffer(handler.onFull(b, controller), b);
    expectBuffer(handler.onFull(b, controller), b);
    /// A completed already; its success must not clear B's new pending marker.
    EXPECT_FALSE(handler.onSuccess(a, controller));
    expectBuffer(handler.onSuccess(b, controller), c);
    EXPECT_FALSE(handler.onSuccess(c, controller));
    EXPECT_TRUE(handler.empty());
}

TEST_F(BackpressureHandlerTest, PendingIdentityIncludesOriginAndChunk)
{
    auto [controller, listener] = createBackpressureChannel();
    BackpressureHandler handler;
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto otherOrigin = makeBuffer(OriginId{2}, SequenceNumber{1}, ChunkNumber{1});
    const auto otherChunk = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{2});

    expectBuffer(handler.onFull(a, controller), a);
    /// A successful buffer with a different origin or chunk must leave A pending.
    EXPECT_FALSE(handler.onSuccess(otherOrigin, controller));
    EXPECT_FALSE(handler.onSuccess(otherChunk, controller));
    EXPECT_FALSE(handler.onFull(otherOrigin, controller));
    EXPECT_FALSE(handler.onFull(otherChunk, controller));
    expectBuffer(handler.onSuccess(a, controller), otherOrigin);
    expectBuffer(handler.onFull(otherOrigin, controller), otherOrigin);
    expectBuffer(handler.onSuccess(otherOrigin, controller), otherChunk);
    EXPECT_FALSE(handler.onSuccess(otherChunk, controller));
    EXPECT_TRUE(handler.empty());
}

TEST_F(BackpressureHandlerTest, UnrelatedSuccessDoesNotReleasePendingBackpressure)
{
    auto [controller, listener] = createBackpressureChannel();
    BackpressureHandler handler(1, 0);
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});

    expectBuffer(handler.onFull(a, controller), a);
    /// applyPressure returns false if pressure is already held, without changing it.
    EXPECT_FALSE(controller.applyPressure());
    EXPECT_FALSE(handler.onSuccess(b, controller));
    EXPECT_FALSE(controller.applyPressure());
    expectBuffer(handler.onFull(a, controller), a);
    EXPECT_FALSE(handler.onSuccess(a, controller));
    /// releasePressure returns false if the handler already released it.
    EXPECT_FALSE(controller.releasePressure());
    /// A fresh failure must start a new retry chain after the old one drained.
    expectBuffer(handler.onFull(b, controller), b);
    EXPECT_FALSE(handler.onSuccess(b, controller));
    EXPECT_FALSE(controller.releasePressure());
}

TEST_F(BackpressureHandlerTest, BackpressureRemainsUntilBacklogDrains)
{
    auto [controller, listener] = createBackpressureChannel();
    BackpressureHandler handler(2, 0);
    const auto a = makeBuffer(OriginId{1}, SequenceNumber{1}, ChunkNumber{1});
    const auto b = makeBuffer(OriginId{1}, SequenceNumber{2}, ChunkNumber{1});
    const auto c = makeBuffer(OriginId{1}, SequenceNumber{3}, ChunkNumber{1});

    expectBuffer(handler.onFull(a, controller), a);
    EXPECT_FALSE(controller.releasePressure());
    EXPECT_FALSE(handler.onFull(b, controller));
    EXPECT_FALSE(controller.releasePressure());
    EXPECT_FALSE(handler.onFull(c, controller));
    EXPECT_FALSE(controller.applyPressure());
    expectBuffer(handler.onSuccess(a, controller), b);
    EXPECT_FALSE(controller.applyPressure());
    expectBuffer(handler.onSuccess(b, controller), c);
    EXPECT_FALSE(handler.onSuccess(c, controller));
    EXPECT_FALSE(controller.releasePressure());
    EXPECT_TRUE(handler.empty());
}
}
}
