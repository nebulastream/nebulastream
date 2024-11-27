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
#include <cstdint>
#include <utility>
#include <Runtime/BufferManager.hpp>
#include <Runtime/FloatingBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include "BufferManagerImpl.hpp"
#include "Runtime/DataSegment.hpp"

#include <BaseUnitTest.hpp>


namespace NES::Memory
{
class BufferManagerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("BufferManagerTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup BufferManager test class.");
    }
};

TEST_F(BufferManagerTest, RunsOutOfBuffers)
{
    std::shared_ptr<BufferManager> const manager = BufferManager::create(1024, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    {
        ASSERT_EQ(manager->getAvailableBuffers(), 1);
        auto const validBuffer = manager->getBufferNoBlocking();
        ASSERT_EQ(manager->getAvailableBuffers(), 0);
        auto const invalidBuffer = manager->getBufferNoBlocking();
        ASSERT_EQ(manager->getAvailableBuffers(), 0);
        ASSERT_TRUE(validBuffer.has_value());
        ASSERT_FALSE(invalidBuffer.has_value());
    }
}

TEST_F(BufferManagerTest, GetDifferentBuffers)
{
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(1024, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    ASSERT_EQ(manager->getAvailableBuffers(), 2);
    auto const buffer1 = manager->getBufferNoBlocking();
    ASSERT_EQ(manager->getAvailableBuffers(), 1);
    auto const buffer2 = manager->getBufferNoBlocking();
    ASSERT_EQ(manager->getAvailableBuffers(), 0);

    ASSERT_NE(buffer1->getBuffer(), buffer2->getBuffer());
}

TEST_F(BufferManagerTest, ReleaseBuffer)
{
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(1024, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);
    {
        auto const buffer = manager->getBufferNoBlocking();
        ASSERT_EQ(manager->getAvailableBuffers(), 0);
    }
    ASSERT_EQ(manager->getAvailableBuffers(), 1);
}

TEST_F(BufferManagerTest, FixedSizePoolRunsOut)
{
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(1024, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    auto const fixedSizeBufferPool = *manager->createFixedSizeBufferPool(1);

    auto const validBuffer = fixedSizeBufferPool->getBufferNoBlocking();
    auto const invalidBuffer = fixedSizeBufferPool->getBufferNoBlocking();

    ASSERT_TRUE(validBuffer.has_value());
    ASSERT_FALSE(invalidBuffer.has_value());
}

TEST_F(BufferManagerTest, SpillBuffer)
{
    constexpr int bufferSize = 1024;
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(bufferSize, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    {
        auto buffer1 = manager->getBufferNoBlocking().value();
        auto* rawData = buffer1.getBuffer();
        std::fill_n(rawData, bufferSize / sizeof(int8_t), 2);
        auto floating = FloatingBuffer(std::move(buffer1));

        {
            auto buffer2 = manager->getBufferBlocking();
            std::fill_n(buffer2.getBuffer(), bufferSize / sizeof(int8_t), 3);
            ASSERT_TRUE(floating.isSpilled());
            ASSERT_EQ(buffer2.getBuffer(), rawData);
        }

        auto const rePinned = manager->pinBuffer(std::move(floating));
        bool restoredData
            = std::all_of(rePinned.getBuffer(), rePinned.getBuffer() + (bufferSize / sizeof(int8_t)), [](int8_t i) { return i == 2; });
        ASSERT_TRUE(restoredData);
        ASSERT_EQ(rePinned.getBuffer(), rawData);
    }
}

TEST_F(BufferManagerTest, StealChildBuffer)
{
    constexpr int bufferSize = 1024;
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(bufferSize, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);
    {
        auto futureChildBuffer = manager->getBufferBlocking();
        auto parentBuffer = manager->getBufferBlocking();
        auto childBuffer = parentBuffer.storeReturnAsChildBuffer(std::move(futureChildBuffer));
        ASSERT_TRUE(childBuffer.has_value());
        ASSERT_EQ(futureChildBuffer.getBuffer(), nullptr);
    }
}

TEST_F(BufferManagerTest, CannotStealChildBuffer)
{
    constexpr int bufferSize = 1024;
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(bufferSize, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);
    {
        auto futureChildBuffer = manager->getBufferBlocking();
        auto childCopy = PinnedBuffer{futureChildBuffer};
        auto parentBuffer = manager->getBufferBlocking();
        auto childBuffer = parentBuffer.storeReturnAsChildBuffer(std::move(futureChildBuffer));
        ASSERT_FALSE(childBuffer.has_value());
    }
}


TEST_F(BufferManagerTest, SpillChildBuffer)
{
    constexpr int bufferSize = 1024;
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(bufferSize, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);
    {
        auto futureChildBuffer = manager->getBufferBlocking();
        auto* rawData = futureChildBuffer.getBuffer();
        std::fill_n(rawData, bufferSize / sizeof(int8_t), 1);
        auto parentBuffer = manager->getBufferBlocking();
        rawData = parentBuffer.getBuffer();
        std::fill_n(rawData, bufferSize / sizeof(int8_t), 2);
        auto childBuffer = parentBuffer.storeReturnAsChildBuffer(std::move(futureChildBuffer));
        auto floatChildBuffer = FloatingBuffer(std::move(*childBuffer));
        auto floatParentBuffer = FloatingBuffer(std::move(parentBuffer));

        {
            auto newPinned = manager->getBufferBlocking();
            rawData = newPinned.getBuffer();
            std::fill_n(rawData, bufferSize / sizeof(int8_t), 3);
        }

        auto repinnedChild = manager->pinBuffer(std::move(floatChildBuffer));
        bool restoredData = std::all_of(
            repinnedChild.getBuffer(), repinnedChild.getBuffer() + (bufferSize / sizeof(int8_t)), [](int8_t i) { return i == 1; });
        ASSERT_TRUE(restoredData);
    }
}

TEST_F(BufferManagerTest, SpillChildBufferClock)
{
    constexpr int bufferSize = 1024;
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(bufferSize, 8, std::make_shared<NesDefaultMemoryAllocator>(), 64, 64, 4);
    {
        auto parentBuffer = manager->getBufferNoBlocking().value();
        auto* rawData = parentBuffer.getBuffer();
        std::fill_n(rawData, bufferSize / sizeof(int8_t), 1);
        std::vector<FloatingBuffer> floatingBuffers{};
        for (int i = 0; i < 7; ++i)
        {
            auto childBuffer = manager->getBufferNoBlocking().value();
            rawData = childBuffer.getBuffer();
            std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 2);
            //Attach buffer1 as child to buffer 2
            childBuffer = *parentBuffer.storeReturnAsChildBuffer(std::move(childBuffer));
            floatingBuffers.push_back(FloatingBuffer{std::move(childBuffer)});
        }
        floatingBuffers.push_back(FloatingBuffer(std::move(parentBuffer)));

        {
            auto buffer1 = manager->getBufferBlocking();
            rawData = buffer1.getBuffer();
            std::fill_n(rawData, bufferSize / sizeof(int8_t), 9);

            bool spilled = false;
            //While spilling should be initiated for the first 4, it might not have processed all completion events
            for (int i = 0; i < 4; ++i)
            {
                if (floatingBuffers[i].isSpilled())
                {
                    spilled = true;
                }
            }
            //last for buffers should definitively not be spilled
            for (int i = 4; i < 8; ++i)
            {
                ASSERT_FALSE(floatingBuffers[i].isSpilled());
            }

            std::vector<PinnedBuffer> pinnedBuffers{};
            for (int i = 0; i < 4; ++i)
            {
                //The first three request will be satisfied with the previously spilled batch, but the fourth request should trigger spilling again.
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 10);
                pinnedBuffers.push_back(nextBuffer);
            }
            for (int i = 0; i < 5; ++i)
            {
                ASSERT_TRUE(floatingBuffers[i].isSpilled());
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            manager->waitForWriteCompletionEventsOnce();
            std::ranges::for_each(floatingBuffers, [](auto it) { ASSERT_TRUE(it.isSpilled()); });
        }
    }
}


TEST_F(BufferManagerTest, GetFreeSegment)
{
    constexpr int bufferSize = 1024;
    std::shared_ptr<BufferManager> const manager
        = BufferManager::create(bufferSize, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    auto segmentFuture = manager->getInMemorySegment();
    auto segment = *segmentFuture.waitUntilDone();

    ASSERT_TRUE(segment.getLocation().getPtr() != nullptr);
}


TEST_F(BufferManagerTest, OnDiskSegmentSpilled)
{
    constexpr uint8_t fileID = 2;
    constexpr unsigned long int offset = 100;
    constexpr unsigned long size = 16 * 1024;
    auto unionSegment = Memory::detail::DataSegment<detail::DataLocation>{
        detail::DataSegment{detail::OnDiskLocation{fileID, offset}, size}};
    auto segment = unionSegment.get<detail::OnDiskLocation>();
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(segment->getLocation().getFileID(), fileID);
    ASSERT_EQ(segment->getLocation().getOffset(), offset);
    ASSERT_EQ(segment->getSize(), size);
}

TEST_F(BufferManagerTest, InMemorySegmentNotSpilled)
{
    uint8_t someData = 12349;
    constexpr unsigned long size = 16 * 1024;
    const auto inMemorySegment = detail::DataSegment{detail::InMemoryLocation{&someData}, size};
    const auto unionSegment = Memory::detail::DataSegment<detail::DataLocation>{inMemorySegment};
    const auto segment = unionSegment.get<detail::InMemoryLocation>();
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(segment->getLocation().getPtr(), &someData);
    ASSERT_EQ(segment->getSize(), size);
}


// TEST_F(FunctionProviderTest, ATest1) {
//     manager->createFixedSizeBufferPool(100);
// }
//
// TEST_F(FunctionProviderTest, ATest2) {
//     manager->createFixedSizeBufferPool(100);
// }

}