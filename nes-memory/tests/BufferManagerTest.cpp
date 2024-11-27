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
#include <array>
#include <cstdint>
#include <queue>
#include <utility>
#include <variant>
#include <vector>

#include <Runtime/BufferManager.hpp>
#include <Runtime/FloatingBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <boost/asio/detail/socket_ops.hpp>

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

    {
        const auto fixedSizeBufferPool = *manager->createFixedSizeBufferPool(1);

        const auto validBuffer = fixedSizeBufferPool->getBufferNoBlocking();
        const auto invalidBuffer = fixedSizeBufferPool->getBufferNoBlocking();

        ASSERT_TRUE(validBuffer.has_value());
        ASSERT_FALSE(invalidBuffer.has_value());
    }
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
        auto floatParentBuffer = FloatingBuffer(std::move(parentBuffer));

        //All child buffers need to be unpinned as well before the main buffer can get spilled
        auto unsuccessfull = manager->getBufferNoBlocking();
        ASSERT_FALSE(unsuccessfull);

        auto floatChildBuffer = FloatingBuffer(std::move(*childBuffer));

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
        std::vector<std::variant<PinnedBuffer, FloatingBuffer>> buffers;
        auto parentBuffer = manager->getBufferNoBlocking().value();
        auto* rawData = parentBuffer.getBuffer();
        std::fill_n(rawData, bufferSize / sizeof(int8_t), 1);
        //Offsets of buffers in this vector should be their offset in allBuffers + 7 (because of the children of the first buffer)
        buffers.push_back(parentBuffer);
        for (int i = 1; i < 8; ++i)
        {
            auto childBuffer = manager->getBufferNoBlocking().value();
            rawData = childBuffer.getBuffer();
            std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 1);
            //Attach buffer1 as child to buffer 2
            childBuffer = *parentBuffer.storeReturnAsChildBuffer(std::move(childBuffer));
            buffers.push_back(FloatingBuffer{std::move(childBuffer)});
        }
        buffers[0] = FloatingBuffer{std::move(parentBuffer)};
        manager->flushNewBuffers();
        ASSERT_EQ(manager->allBuffers.size(), 8);
        {
            buffers.push_back(manager->getBufferBlocking());
            rawData = std::get<PinnedBuffer>(buffers[8]).getBuffer();
            std::fill_n(rawData, bufferSize / sizeof(int8_t), 9);

            bool spilled = false;
            //While spilling should be initiated for the first 4, it might not have processed all completion events
            //At least one must have been spilled
            for (int i = 1; i < 4; ++i)
            {
                if (std::get<FloatingBuffer>(buffers[i]).isSpilled())
                {
                    spilled = true;
                }
            }
            ASSERT_TRUE(spilled);
            //The segments getting spilled are all children, so the clock should go around once,
            //stop again at 0 and not move until all associated segments are spilled
            ASSERT_EQ(manager->clockAt, 0);
            //last five buffers should definitively not be spilled
            for (int i = 4; i < 8; ++i)
            {
                ASSERT_FALSE(std::get<FloatingBuffer>(buffers[i]).isSpilled());
            }
            ASSERT_FALSE(std::get<FloatingBuffer>(buffers[0]).isSpilled());

            //Second chance spilling should also clean up unused BCBs (the ones which data segment we attached as children to the first one)
            //in its first pass
            manager->flushNewBuffers();
            ASSERT_EQ(manager->allBuffers.size(), 2);

            for (int i = 9; i < 12; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 1);
                buffers.push_back(nextBuffer);
            }
            for (int i = 1; i < 4; ++i)
            {
                ASSERT_TRUE(std::get<FloatingBuffer>(buffers[i]).isSpilled());
            }
            spilled = false;
            for (int i = 4; i < 7; ++i)
            {
                if (std::get<FloatingBuffer>(buffers[i]).isSpilled())
                {
                    spilled = true;
                }
            }
            ASSERT_TRUE(spilled);
            ASSERT_EQ(manager->clockAt, 0);
            manager->flushNewBuffers();
            ASSERT_EQ(manager->allBuffers.size(), 5);

            //Spill the rest of the child buffers + parent buffer
            for (int i = 12; i < 16; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 1);
                buffers.push_back(nextBuffer);
            }
            std::ranges::for_each(
                buffers.begin(), buffers.begin() + 8, [](const auto& it) { ASSERT_TRUE(std::get<FloatingBuffer>(it).isSpilled()); });
            //All children and the parent are spilled, so the clock should move forward
            ASSERT_EQ(manager->clockAt, 1);

            manager->flushNewBuffers();
            ASSERT_EQ(manager->allBuffers.size(), 9);

            //Unpin last four buffers, then initiate spilling once, where second chance should take 5, 6, 7 and tag 8,
            //Again, offsets are increased by 7 because of the child buffers
            for (int i = 12; i < 16; ++i)
            {
                buffers[i] = FloatingBuffer{std::move(std::get<PinnedBuffer>(buffers[i]))};
            }

            for (int i = 16; i < 19; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 1);
                buffers.push_back(nextBuffer);
            }

            std::ranges::for_each(
                buffers.begin() + 12, buffers.begin() + 15, [](const auto& it) { ASSERT_TRUE(std::get<FloatingBuffer>(it).isSpilled()); });
            ASSERT_EQ(manager->clockAt, 8);
            ASSERT_FALSE(std::get<FloatingBuffer>(buffers[15]).isSpilled());

            //Unpin (by allBuffers offset), 1, 2, 3
            //Spilled should be 8, 1, 2, because 8 was already tagged before
            for (int i = 8; i < 11; ++i)
            {
                buffers[i] = FloatingBuffer{std::move(std::get<PinnedBuffer>(buffers[i]))};
            }

            for (int i = 19; i < 22; ++i)
            {
                auto nextBuffer = manager->getBufferBlocking();
                rawData = nextBuffer.getBuffer();
                std::fill_n(rawData, bufferSize / sizeof(int8_t), i + 1);
                buffers.push_back(nextBuffer);
            }

            ASSERT_EQ(manager->clockAt, 3);
            manager->flushNewBuffers();
            ASSERT_EQ(manager->allBuffers.size(), 15);

            ASSERT_TRUE(std::get<FloatingBuffer>(buffers[15]).isSpilled());
            //floatingBuffers[14] is allBuffers[3]
            ASSERT_FALSE(std::get<FloatingBuffer>(buffers[10]).isSpilled());
        }
    }
}

TEST_F(BufferManagerTest, BufferCleanupClock)
{
    constexpr int bufferSize = 1024;
    const std::shared_ptr<BufferManager> manager
        = BufferManager::create(bufferSize, 10, std::make_shared<NesDefaultMemoryAllocator>(), 64, 2, 4);

    {
        std::vector<PinnedBuffer> buffers{};
        for (int i = 0; i < 10; ++i)
        {
            buffers.push_back(manager->getBufferBlocking());
        }
    }
}

TEST_F(BufferManagerTest, GetFreeSegment)
{
    constexpr int bufferSize = 1024;
    const std::shared_ptr<BufferManager> manager = BufferManager::create(bufferSize, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);

    auto segmentFuture = manager->getInMemorySegment(1);
    const auto segment = std::get<std::vector<detail::DataSegment<detail::InMemoryLocation>>>(segmentFuture.waitUntilDone())[0];

    ASSERT_TRUE(segment.getLocation().getPtr() != nullptr);
}


TEST_F(BufferManagerTest, AttachUnpooledBufferAsChild)
{
    constexpr int bufferSize = 1024;
    const std::shared_ptr<BufferManager> manager = BufferManager::create(bufferSize, 1, std::make_shared<NesDefaultMemoryAllocator>(), 64);
    {
        auto parent = manager->getBufferBlocking();
        auto rawData = parent.getBuffer();
        std::fill_n(rawData, bufferSize / sizeof(int8_t), 1);

        auto toBeChild = *manager->getUnpooledBuffer(bufferSize * 16);
        rawData = toBeChild.getBuffer();
        std::fill_n(rawData, (bufferSize * 16) / sizeof(int8_t), 2);

        auto childFloating = FloatingBuffer{*parent.storeReturnAsChildBuffer(std::move(toBeChild))};
        auto parentFloating = FloatingBuffer{std::move(parent)};

        {
            auto thirdBuffer = manager->getBufferBlocking();
            rawData = thirdBuffer.getBuffer();
            std::fill_n(rawData, bufferSize / sizeof(int8_t), 3);
        }
        auto repinnedChild = *getOptional<PinnedBuffer>(manager->repinBuffer(std::move(childFloating)).waitUntilDone());

        bool restoredChildData = std::all_of(
            repinnedChild.getBuffer(), repinnedChild.getBuffer() + (bufferSize * 16 / sizeof(int8_t)), [](int8_t i) { return i == 2; });
        ASSERT_TRUE(restoredChildData);

        auto repinnedParent = *getOptional<PinnedBuffer>(manager->repinBuffer(std::move(parentFloating)).waitUntilDone());

        bool restoredParentData = std::all_of(
            repinnedParent.getBuffer(), repinnedParent.getBuffer() + (bufferSize / sizeof(int8_t)), [](int8_t i) { return i == 1; });
        ASSERT_TRUE(restoredParentData);
    }
}

TEST_F(BufferManagerTest, MultiThreadedSpilling)
{
    constexpr int bufferSize = 1024 * 8;
    constexpr int numThreads = 16;
    constexpr int allocatePerThread = 1000;
    constexpr int inMemorySegmentsPerThread = 10;
    constexpr int inMemoryReserve = 1;
    const std::shared_ptr<BufferManager> manager = BufferManager::create(
        bufferSize, numThreads * inMemorySegmentsPerThread, std::make_shared<NesDefaultMemoryAllocator>(), 64, 2, 1024, 1024, 1024);

    {
        const std::stop_source stopSource;
        std::array<std::vector<FloatingBuffer>, numThreads> floatingResults{};
        std::array<std::thread, numThreads> threads{};

        auto threadFunction = [&, stopToken = stopSource.get_token()](const int threadNum)
        {
            std::queue<PinnedBuffer> pinnedBuffers{};
            std::vector<FloatingBuffer> floatingBuffers{};
            floatingBuffers.reserve(allocatePerThread);

            for (int i = 0; i < allocatePerThread; ++i)
            {
                if (i >= (inMemorySegmentsPerThread - inMemoryReserve))
                {
                    floatingBuffers.push_back(FloatingBuffer{std::move(pinnedBuffers.front())});
                    pinnedBuffers.pop();
                }
                auto pinned = manager->getBufferBlocking();

                auto rawData = reinterpret_cast<uint64_t*>(pinned.getBuffer());
                std::fill_n(rawData, bufferSize / sizeof(uint64_t), threadNum * allocatePerThread + i);
                pinnedBuffers.push(pinned);
            }

            while (!pinnedBuffers.empty())
            {
                floatingBuffers.push_back(FloatingBuffer{std::move(pinnedBuffers.front())});
                pinnedBuffers.pop();
            }
            floatingResults[threadNum] = floatingBuffers;

            while (!stopToken.stop_requested())
            {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        };

        for (int i = 0; i < numThreads; ++i)
        {
            threads[i] = std::thread{threadFunction, i};
        }

        bool done = false;
        while (!done)
        {
            bool previousThreadsFinished = true;
            for (auto threadResult : floatingResults)
            {
                if (threadResult.size() != allocatePerThread)
                {
                    previousThreadsFinished = false;
                    break;
                }
            }
            done = previousThreadsFinished;
            if (!done)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        ASSERT_TRUE(stopSource.request_stop());

        for (int i = 0; i < numThreads; ++i)
        {
            threads[i].join();
        }
        NES_INFO("Done spilling")

        NES_INFO("Starting to verify results")
        for (unsigned int threadNum = 0; threadNum < numThreads; ++threadNum)
        {
            for (unsigned int bufferNum = 0; bufferNum < floatingResults.size(); ++bufferNum)
            {
                auto floatingBuffer = floatingResults[threadNum][bufferNum];
                auto repinningResult = manager->repinBuffer(std::move(floatingBuffer)).waitUntilDone();
                ASSERT_TRUE(std::holds_alternative<PinnedBuffer>(repinningResult));
                auto pinnedBuffer = std::get<PinnedBuffer>(repinningResult);

                bool restoredParentData = std::all_of(
                    reinterpret_cast<uint64_t*>(pinnedBuffer.getBuffer()),
                    reinterpret_cast<uint64_t*>(pinnedBuffer.getBuffer()) + (bufferSize / sizeof(uint64_t)),
                    [&](uint64_t i)
                    {
                        bool check = i == threadNum * allocatePerThread + bufferNum;
                        if (!check)
                        {
                            NES_DEBUG("Expected {}, but value was {}", threadNum * allocatePerThread + bufferNum, i);
                        }
                        return check;
                    });
                ASSERT_TRUE(restoredParentData);
            }
        }
    }
}

TEST_F(BufferManagerTest, RepinUnpinContention)
{
    constexpr int bufferSize = 1024 * 8;
    constexpr int numThreads = 10;
    constexpr int concurrentBufferAmount = 2;
    constexpr int inMemoryBuffers = 1;
    constexpr int maxRepinnings = 500;
    const std::shared_ptr<BufferManager> manager
        = BufferManager::create(bufferSize, inMemoryBuffers, std::make_shared<NesDefaultMemoryAllocator>(), 64, 2, 1024, 1024, 1024);

    {
        folly::MPMCQueue<detail::CoroutineError> coroErrors{maxRepinnings * numThreads};
        std::array<std::thread, numThreads> threads{};
        const std::stop_source stopSource;
        std::array<std::atomic_flag, numThreads> threadsDone{};
        std::array<FloatingBuffer, concurrentBufferAmount> buffers{};
        for (unsigned int i = 0; i < concurrentBufferAmount; ++i)
        {
            auto pinned = manager->getBufferBlocking();
            auto rawData = reinterpret_cast<uint64_t*>(pinned.getBuffer());
            std::fill_n(rawData, bufferSize / sizeof(uint64_t), i);
            buffers[i] = FloatingBuffer{std::move(pinned)};
        }
        std::vector<detail::DataSegment<detail::DataLocation>> segmentsForValidation{};
        for (auto buffer : buffers)
        {
            segmentsForValidation.push_back(buffer.getSegment());
        }

        // std::array<std::atomic<uint32_t>, concurrentBufferAmount> repinnings{};
        // repinnings.fill(maxRepinnings - 1);


        auto threadFunction = [&, stopToken = stopSource.get_token()](const unsigned int threadNum)
        {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> range(0, concurrentBufferAmount - 1);
            for (int i = 0; i < maxRepinnings; ++i)
            {
                int num = range(gen);
                //create copy of the floating buffer so that we don't invalidate the floating buffer inside the array when we move it
                auto floating = buffers[num];
                auto pinnedResult = manager->repinBuffer(std::move(floating)).waitUntilDone();
                if (std::holds_alternative<detail::CoroutineError>(pinnedResult))
                {
                    coroErrors.write(std::get<detail::CoroutineError>(pinnedResult));
                }
            }

            threadsDone[threadNum].test_and_set();
            threadsDone[threadNum].notify_one();
            while (!stopToken.stop_requested())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        };
        for (unsigned int i = 0; i < numThreads; ++i)
        {
            threads[i] = std::thread{threadFunction, i};
        }

        for (auto& done : threadsDone)
        {
            done.wait(false);
        }

        ASSERT_TRUE(stopSource.request_stop());

        for (int i = 0; i < numThreads; ++i)
        {
            threads[i].join();
        }
        NES_INFO("Starting validation")

        unsigned int inMemory = 0;
        for (auto buffer : buffers)
        {
            if (!buffer.isSpilled())
            {
                inMemory++;
            }
        }

        ASSERT_EQ(inMemory, inMemoryBuffers);

        for (unsigned int i = 0; i < concurrentBufferAmount; ++i)
        {
            auto floating = buffers[i];
            auto repinningResult = manager->repinBuffer(std::move(floating)).waitUntilDone();
            ASSERT_TRUE(std::holds_alternative<PinnedBuffer>(repinningResult));
            auto pinned = std::get<PinnedBuffer>(repinningResult);
            bool restoredParentData = std::all_of(
                reinterpret_cast<uint64_t*>(pinned.getBuffer()),
                reinterpret_cast<uint64_t*>(pinned.getBuffer()) + (bufferSize / sizeof(uint64_t)),
                [&](const uint64_t val)
                {
                    bool check = val == i;
                    if (!check)
                    {
                        NES_DEBUG("Expected {}, but value was {}", i, val);
                    }
                    return check;
                });
            ASSERT_TRUE(restoredParentData);
        }
        std::map<detail::CoroutineError, uint64_t> errorCounter{};
        detail::CoroutineError error;
        while (coroErrors.read(error))
        {
            auto counterEntry = errorCounter.find(error);
            if (counterEntry == errorCounter.end())
            {
                errorCounter.emplace(error, 1);
            }
            else
            {
                counterEntry->second++;
            }
        }
        unsigned int sum = 0;
        for (auto [errorCode, counter] : errorCounter)
        {
            NES_INFO("Error \"{}\" was encountered {} times", ErrorCode::typeFromCode(errorCode).getErrorMessage(), counter);
            sum += counter;
        }
        NES_INFO("{} out of {} attempts to repin failed", sum, maxRepinnings * numThreads);
    }
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

TEST_F(BufferManagerTest, NotPreAllocatedSegment)
{
    uint8_t someData = 12349;
    constexpr unsigned long size = 16 * 1024;
    const auto inMemorySegment = detail::DataSegment{detail::InMemoryLocation{&someData, true}, size};
    const auto unionSegment = Memory::detail::DataSegment<detail::DataLocation>{inMemorySegment};
    const auto segment = unionSegment.get<detail::InMemoryLocation>();
    ASSERT_TRUE(segment.has_value());
    ASSERT_EQ(segment->getLocation().getPtr(), &someData);
    ASSERT_EQ(segment->getSize(), size);
    ASSERT_TRUE(segment->isNotPreAllocated());
}


// TEST_F(FunctionProviderTest, ATest1) {
//     manager->createFixedSizeBufferPool(100);
// }
//
// TEST_F(FunctionProviderTest, ATest2) {
//     manager->createFixedSizeBufferPool(100);
// }
}