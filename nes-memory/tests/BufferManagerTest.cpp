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

#include <Runtime/BufferManagerImpl.hpp>
#include <Runtime/DataSegment.hpp>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

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
    const std::shared_ptr<BufferManager> manager = BufferManager::create(1024, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    {
        ASSERT_EQ(manager->getAvailableBuffers(), 1);
        const auto validBuffer = manager->getBufferNoBlocking();
        ASSERT_EQ(manager->getAvailableBuffers(), 0);
        const auto invalidBuffer = manager->getBufferNoBlocking();
        ASSERT_EQ(manager->getAvailableBuffers(), 0);
        ASSERT_TRUE(validBuffer.has_value());
        ASSERT_FALSE(invalidBuffer.has_value());
    }
}

TEST_F(BufferManagerTest, GetDifferentBuffers)
{
    const std::shared_ptr<BufferManager> manager = BufferManager::create(1024, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    ASSERT_EQ(manager->getAvailableBuffers(), 2);
    const auto buffer1 = manager->getBufferNoBlocking();
    ASSERT_EQ(manager->getAvailableBuffers(), 1);
    const auto buffer2 = manager->getBufferNoBlocking();
    ASSERT_EQ(manager->getAvailableBuffers(), 0);

    ASSERT_NE(buffer1->getBuffer(), buffer2->getBuffer());
}

TEST_F(BufferManagerTest, ReleaseBuffer)
{
    const std::shared_ptr<BufferManager> manager = BufferManager::create(1024, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);
    {
        const auto buffer = manager->getBufferNoBlocking();
        ASSERT_EQ(manager->getAvailableBuffers(), 0);
    }
    ASSERT_EQ(manager->getAvailableBuffers(), 1);
}

TEST_F(BufferManagerTest, FixedSizePoolRunsOut)
{
    const std::shared_ptr<BufferManager> manager = BufferManager::create(1024, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    const auto fixedSizeBufferPool = *manager->createFixedSizeBufferPool(1);

    const auto validBuffer = fixedSizeBufferPool->getBufferNoBlocking();
    const auto invalidBuffer = fixedSizeBufferPool->getBufferNoBlocking();

    ASSERT_TRUE(validBuffer.has_value());
    ASSERT_FALSE(invalidBuffer.has_value());
}

TEST_F(BufferManagerTest, SpillBuffer)
{
    constexpr int bufferSize = 1024;
    const std::shared_ptr<BufferManager> manager = BufferManager::create(bufferSize, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);

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

        const auto rePinned = manager->pinBuffer(std::move(floating));
        bool restoredData
            = std::all_of(rePinned.getBuffer(), rePinned.getBuffer() + (bufferSize / sizeof(int8_t)), [](int8_t i) { return i == 2; });
        ASSERT_TRUE(restoredData);
        ASSERT_EQ(rePinned.getBuffer(), rawData);
    }
}

TEST_F(BufferManagerTest, StealChildBuffer)
{
    constexpr int bufferSize = 1024;
    const std::shared_ptr<BufferManager> manager = BufferManager::create(bufferSize, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);
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
    const std::shared_ptr<BufferManager> manager = BufferManager::create(bufferSize, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);
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
    const std::shared_ptr<BufferManager> manager = BufferManager::create(bufferSize, 2, std::make_shared<NesDefaultMemoryAllocator>(), 64);
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
    const std::shared_ptr<BufferManager> manager
        = BufferManager::create(bufferSize, 8, std::make_shared<NesDefaultMemoryAllocator>(), 64, 2, 128, 64, 3);
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

        ASSERT_EQ(manager->allBuffers.size(), 8);
        {
            std::vector<PinnedBuffer> pinnedBuffers{};
            pinnedBuffers.push_back(manager->getBufferBlocking());
            rawData = pinnedBuffers[0].getBuffer();
            std::fill_n(rawData, bufferSize / sizeof(int8_t), 9);

            bool spilled = false;
            //While spilling should be initiated for the first 4, it might not have processed all completion events
            //At least one must have been spilled
            for (int i = 0; i < 3; ++i)
            {
                if (floatingBuffers[i].isSpilled())
                {
                    spilled = true;
                }
            }
            ASSERT_TRUE(spilled);
            //The segments getting spilled are all children, so the clock should go around once,
            //stop again at 0 and not move until all associated segments are spilled
            ASSERT_EQ(manager->clockAt, 0);
            //last for buffers should definitively not be spilled
            for (int i = 3; i < 8; ++i)
            {
                ASSERT_FALSE(floatingBuffers[i].isSpilled());
            }

            //Second chance spilling should also clean up unused BCBs (the ones which data segment we attached as children to the first one)
            //in its first pass
            ASSERT_EQ(manager->allBuffers.size(), 2);

            for (int i = 0; i < 3; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 10);
                pinnedBuffers.push_back(nextBuffer);
            }
            for (int i = 0; i < 3; ++i)
            {
                ASSERT_TRUE(floatingBuffers[i].isSpilled());
            }
            spilled = false;
            for (int i = 3; i < 8; ++i)
            {
                if (floatingBuffers[i].isSpilled())
                {
                    spilled = true;
                }
            }
            ASSERT_TRUE(spilled);
            ASSERT_EQ(manager->clockAt, 0);
            ASSERT_EQ(manager->allBuffers.size(), 5);

            //Spill the rest of the child buffers
            for (int i = 0; i < 4; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 13);
                pinnedBuffers.push_back(nextBuffer);
            }
            std::ranges::for_each(floatingBuffers, [](const auto& it) { ASSERT_TRUE(it.isSpilled()); });
            //All children and the parent are spilled, so the clock should move forward
            ASSERT_EQ(manager->clockAt, 1);
            ASSERT_EQ(manager->allBuffers.size(), 9);

            //Unpin last four buffers, then initiate spilling once, where second chance should take pinned (starting at zero) 4, 5, 6 and tag 7,
            //Offset of these buffers in allBuffers are 5, 6, 7, tag 8.
            for (int i = 4; i < 8; ++i)
            {
                floatingBuffers.emplace_back(std::move(pinnedBuffers[i]));
            }
            pinnedBuffers.erase(pinnedBuffers.begin() + 4, pinnedBuffers.end());

            for (int i = 0; i < 3; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 17);
                pinnedBuffers.push_back(nextBuffer);
            }

            std::ranges::for_each(floatingBuffers.begin(), floatingBuffers.end() - 1, [](const auto& it) { ASSERT_TRUE(it.isSpilled()); });
            ASSERT_FALSE(floatingBuffers[11].isSpilled());
            ASSERT_EQ(manager->clockAt, 8);

            //Unpin (by allBuffers offset), 1, 2, 3
            //Spilled should be 8, 1, 2, because 8 was already tagged before
            floatingBuffers.emplace_back(std::move(pinnedBuffers[0]));
            floatingBuffers.emplace_back(std::move(pinnedBuffers[1]));
            floatingBuffers.emplace_back(std::move(pinnedBuffers[2]));
            pinnedBuffers.erase(pinnedBuffers.begin(), pinnedBuffers.begin() + 3); //End index is exclusive

            for (int i = 0; i < 3; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 20);
                pinnedBuffers.push_back(nextBuffer);
            }

            ASSERT_EQ(manager->clockAt, 3);
            ASSERT_EQ(manager->allBuffers.size(), 15);

            ASSERT_TRUE(floatingBuffers[11].isSpilled());
            //floatingBuffers[14] is allBuffers[3]
            ASSERT_FALSE(floatingBuffers[14].isSpilled());
        }
    }
}

TEST_F(BufferManagerTest, BufferCleanupClock)
{
    constexpr int bufferSize = 1024;
    const std::shared_ptr<BufferManager> manager
        = BufferManager::create(bufferSize, 8, std::make_shared<NesDefaultMemoryAllocator>(), 64, 2, 4);
}

TEST_F(BufferManagerTest, GetFreeSegment)
{
    constexpr int bufferSize = 1024;
    const std::shared_ptr<BufferManager> manager = BufferManager::create(bufferSize, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    auto segmentFuture = manager->getInMemorySegment();
    auto segment = *segmentFuture.waitUntilDone();

    ASSERT_TRUE(segment.getLocation().getPtr() != nullptr);
}


TEST_F(BufferManagerTest, OnDiskSegmentSpilled)
{
    constexpr uint8_t fileID = 2;
    constexpr unsigned long int offset = 100;
    constexpr unsigned long size = 16 * 1024;
    auto unionSegment
        = Memory::detail::DataSegment<detail::DataLocation>{detail::DataSegment{detail::OnDiskLocation{fileID, offset}, size}};
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