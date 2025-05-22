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
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <random>
#include <thread>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

using namespace std;

namespace NES
{

struct ChunkStateTest
{
    uint64_t lastChunkNumber = ChunkNumber::INVALID;
    uint64_t seenChunks = 0;
    uint64_t value = 0;
};

class NonBlockingMonotonicSeqQueueTest : public Testing::BaseUnitTest
{
public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("NonBlockingMonotonicSeqQueueTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NonBlockingMonotonicSeqQueueTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        watermarkBarriers.clear();
    }

    /**
     * @brief Emplaces into a mock queue that is not concurrent thread-safe
     * @param seqDataToInsert
     * @param value
     * @return CurrentValue
     */
    uint64_t emplaceInMockupQueue(const SequenceData& seqDataToInsert, const uint64_t value)
    {
        /// Implementing a mock-up of a MonotonicSequenceQueue
        auto& chunkState = seenSequenceData[seqDataToInsert.sequenceNumber];
        if (seqDataToInsert.lastChunk)
        {
            chunkState.lastChunkNumber = seqDataToInsert.chunkNumber;
        }
        chunkState.seenChunks++;
        chunkState.value = std::max(value, chunkState.value);

        /// Checking what is the maximum sequence number that we have seen all chunks
        uint64_t currentValue = 0;
        auto nextSeqNumber = SequenceNumber::INITIAL;
        auto chunkStateNextSeq = seenSequenceData.find(nextSeqNumber);
        while (chunkStateNextSeq != seenSequenceData.end())
        {
            if (chunkStateNextSeq->second.seenChunks - 1 != chunkStateNextSeq->second.lastChunkNumber - ChunkNumber::INITIAL)
            {
                break;
            }
            currentValue = chunkStateNextSeq->second.value;
            ++nextSeqNumber;
            chunkStateNextSeq = seenSequenceData.find(nextSeqNumber);
        }

        return currentValue;
    }

    std::map<SequenceNumber::Underlying, ChunkStateTest> seenSequenceData;
    std::vector<std::tuple<SequenceData, uint64_t>> watermarkBarriers;
};

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a sequential list of 10k updates, monotonically increasing from 1 to 10k and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a sequential fashion we assume that the getCurrentWatermark is equal to the latest processed update.
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, singleThreadSequentialUpdaterTest)
{
    auto updates = 10000_u64;
    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>();
    /// preallocate watermarks for each transaction
    for (auto i = SequenceNumber::INITIAL; i <= updates; i++)
    {
        watermarkBarriers.emplace_back(
            std::tuple<SequenceData, uint64_t>(/*sequence data*/ {SequenceNumber(i), INITIAL<ChunkNumber>, true}, /*ts*/ i));
    }
    for (auto i = 0_u64; i < updates; i++)
    {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_LT(oldWatermark, std::get<1>(currentWatermarkBarrier));
        watermarkProcessor.emplace(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<1>(currentWatermarkBarrier));
    }
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<1>(watermarkBarriers.back()));
}

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a reverse sequential list of 10k updates, monotonically decreasing from 10k to 1 and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a sequential fashion we assume that the getCurrentWatermark is equal to the latest processed update.
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, singleThreadReversSequentialUpdaterTest)
{
    auto updates = 10000_u64;
    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>();
    /// preallocate watermarks for each transaction
    for (auto i = SequenceNumber::INITIAL; i <= updates; i++)
    {
        watermarkBarriers.emplace_back(
            std::tuple<SequenceData, uint64_t>(/*sequence data*/ {SequenceNumber(i), INITIAL<ChunkNumber>, true}, /*ts*/ i));
    }
    /// reverse updates
    std::ranges::reverse(watermarkBarriers);

    for (auto i = 0_u64; i < updates - 1; i++)
    {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_LT(oldWatermark, std::get<1>(currentWatermarkBarrier));
        watermarkProcessor.emplace(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
        ASSERT_EQ(watermarkProcessor.getCurrentValue(), 0);
    }
    /// add the last remaining watermark, as a result we now apply all remaining watermarks.
    watermarkProcessor.emplace(std::get<0>(watermarkBarriers.back()), std::get<1>(watermarkBarriers.back()));
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<1>(watermarkBarriers.front()));
}

/**
 * @brief A single thread test for the lock free watermark processor.
 * We create a reverse sequential list of 10k updates, monotonically decreasing from 10k to 1 and push them to the watermark processor.
 * Assumption:
 * As we insert all updates in a sequential fashion we assume that the getCurrentWatermark is equal to the latest processed update.
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, singleThreadRandomeUpdaterTest)
{
    auto updates = 100_u64;
    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>();
    /// preallocate watermarks for each transaction
    for (auto i = SequenceNumber::INITIAL; i <= updates; i++)
    {
        watermarkBarriers.emplace_back(
            std::tuple<SequenceData, uint64_t>(/*sequence data*/ {SequenceNumber(i), INITIAL<ChunkNumber>, true}, /*ts*/ i));
    }
    std::mt19937 randomGenerator(42);
    std::shuffle(watermarkBarriers.begin(), watermarkBarriers.end(), randomGenerator);

    for (auto i = 0_u64; i < updates; i++)
    {
        auto currentWatermarkBarrier = watermarkBarriers[i];
        auto oldWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_LT(oldWatermark, std::get<1>(currentWatermarkBarrier));
        watermarkProcessor.emplace(std::get<0>(currentWatermarkBarrier), std::get<1>(currentWatermarkBarrier));
    }
    /// add the last remaining watermark, as a result we now apply all remaining watermarks.
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), updates);
}

TEST_F(NonBlockingMonotonicSeqQueueTest, concurrentLockFreeWatermarkUpdaterTest)
{
    const auto updates = 100000;
    const auto threadsCount = 10;
    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t, 10000>();

    /// preallocate watermarks for each transaction
    for (auto i = SequenceNumber::INITIAL; i <= updates * threadsCount; i++)
    {
        watermarkBarriers.emplace_back(
            std::tuple<SequenceData, uint64_t>(/*sequence data*/ {SequenceNumber(i), INITIAL<ChunkNumber>, true}, /*ts*/ i));
    }
    std::atomic<uint64_t> globalUpdateCounter = 0;
    std::vector<std::thread> threads;
    threads.reserve(threadsCount);
    for (int threadId = 0; threadId < threadsCount; threadId++)
    {
        threads.emplace_back(
            [&watermarkProcessor, this, &globalUpdateCounter]()
            {
                /// each thread processes a particular update
                for (auto i = 0; i < updates; i++)
                {
                    auto currentWatermark = watermarkBarriers[globalUpdateCounter++];
                    auto oldWatermark = watermarkProcessor.getCurrentValue();
                    /// check if the watermark manager does not return a watermark higher than the current one
                    ASSERT_LT(oldWatermark, std::get<1>(currentWatermark));
                    watermarkProcessor.emplace(std::get<0>(currentWatermark), std::get<1>(currentWatermark));
                    /// check that the watermark manager returns a watermark that is <= to the max watermark
                    auto globalCurrentWatermark = watermarkProcessor.getCurrentValue();
                    auto maxCurrentWatermark = watermarkBarriers[globalUpdateCounter - 1];
                    ASSERT_LE(globalCurrentWatermark, std::get<1>(maxCurrentWatermark));
                }
            });
    }

    for (auto& thread : threads)
    {
        thread.join();
    }
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<1>(watermarkBarriers.back()));
}

TEST_F(NonBlockingMonotonicSeqQueueTest, concurrentUpdatesWithLostUpdateThreadTest)
{
    const auto updates = 10000;
    const auto lostUpdate = 666;
    const auto threadsCount = 10;
    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t, 1000>();

    /// preallocate watermarks for each transaction
    for (auto i = SequenceNumber::INITIAL; i <= updates * threadsCount; i++)
    {
        watermarkBarriers.emplace_back(
            std::tuple<SequenceData, uint64_t>(/*sequence data*/ {SequenceNumber(i), INITIAL<ChunkNumber>, true}, /*ts*/ i));
    }
    std::atomic<uint64_t> globalUpdateCounter = 0;
    std::vector<std::thread> threads;
    threads.reserve(threadsCount);
    for (int threadId = 0; threadId < threadsCount; threadId++)
    {
        threads.emplace_back(
            [&watermarkProcessor, this, &globalUpdateCounter]()
            {
                /// each thread processes a particular update
                for (auto i = 0; i < updates; i++)
                {
                    auto nextUpdate = globalUpdateCounter++;
                    if (nextUpdate == lostUpdate)
                    {
                        continue;
                    }
                    auto currentWatermark = watermarkBarriers[nextUpdate];
                    auto oldWatermark = watermarkProcessor.getCurrentValue();
                    /// check if the watermark manager does not return a watermark higher than the current one
                    ASSERT_LT(oldWatermark, std::get<1>(watermarkBarriers[lostUpdate]));
                    watermarkProcessor.emplace(std::get<0>(currentWatermark), std::get<1>(currentWatermark));
                    /// check that the watermark manager returns a watermark that is <= to the max watermark
                    auto globalCurrentWatermark = watermarkProcessor.getCurrentValue();
                    ASSERT_LE(globalCurrentWatermark, std::get<1>(watermarkBarriers[lostUpdate]));
                }
            });
    }

    for (auto& thread : threads)
    {
        thread.join();
    }
    auto currentValue = watermarkProcessor.getCurrentValue();
    ASSERT_EQ(currentValue, std::get<1>(watermarkBarriers[lostUpdate - 1]));
    watermarkProcessor.emplace(std::get<0>(watermarkBarriers[lostUpdate]), std::get<1>(watermarkBarriers[lostUpdate]));

    ASSERT_EQ(watermarkProcessor.getCurrentValue(), std::get<1>(watermarkBarriers.back()));
}

/**
 * @brief We test here to insert sequence and chunks numbers in a "random" fashion and then check, if the correct output
 * is produced
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, singleThreadedUpdatesWithChunkNumberInRandomFashionTest)
{
    auto noSeqNumbers = 10000_u64;
    auto maxChunksPerSeqNumber = 20_u64;
    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>();
    /// preallocate watermarks for each transaction
    for (auto i = SequenceNumber::INITIAL; i <= noSeqNumbers; i++)
    {
        auto noChunks = 1 + (rand() % maxChunksPerSeqNumber);
        for (auto chunk = ChunkNumber::INITIAL; chunk < ChunkNumber::INITIAL + noChunks; ++chunk)
        {
            watermarkBarriers.emplace_back(
                std::tuple<SequenceData, uint64_t>(/*sequence data*/ {SequenceNumber(i), ChunkNumber(chunk), false}, /*ts*/ i));
        }
        watermarkBarriers.emplace_back(std::tuple<SequenceData, uint64_t>(
            /*sequence data*/ {SequenceNumber(i), ChunkNumber(noChunks + ChunkNumber::INITIAL), true},
            /*ts*/ i));
    }

    std::mt19937 randomGenerator(42);
    std::shuffle(watermarkBarriers.begin(), watermarkBarriers.end(), randomGenerator);

    for (const auto& currentWatermarkBarrier : watermarkBarriers)
    {
        const auto& seqDataToInsert = std::get<0>(currentWatermarkBarrier);
        const auto& valueToInsert = std::get<1>(currentWatermarkBarrier);

        /// Emplacing in mock-up queue
        auto currentValueExpected = emplaceInMockupQueue(seqDataToInsert, valueToInsert);

        /// Checking the new watermark, after emplacing the current
        watermarkProcessor.emplace(seqDataToInsert, valueToInsert);
        auto newWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_EQ(newWatermark, currentValueExpected);
    }
    /// add the last remaining watermark, as a result we now apply all remaining watermarks.
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), noSeqNumbers);
}

/**
 * @brief We test here to insert sequence and chunks numbers in a "random" fashion and then check, if the correct output
 * is produced. We do this in a concurrent fashion
 */
TEST_F(NonBlockingMonotonicSeqQueueTest, concurrentUpdatesWithChunkNumberInRandomFashionTest)
{
    constexpr auto blockSize = 100_u64;
    constexpr auto noSeqNumbers = 10000_u64;
    constexpr auto averageUpdatesPerRound = 100_u64;
    constexpr auto threadsCount = 10;
    constexpr auto maxChunksPerSeqNumber = 20_u64;
    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t, blockSize>();
    /// preallocate watermarks for each transaction
    for (auto i = SequenceNumber::INITIAL; i < noSeqNumbers + SequenceNumber::INITIAL; i++)
    {
        auto noChunks = 1 + (rand() % maxChunksPerSeqNumber);
        for (auto chunk = ChunkNumber::INITIAL; chunk < noChunks + ChunkNumber::INITIAL; ++chunk)
        {
            watermarkBarriers.emplace_back(
                std::tuple<SequenceData, uint64_t>(/*sequence data*/ {SequenceNumber(i), ChunkNumber(chunk), false}, /*ts*/ i));
        }
        watermarkBarriers.emplace_back(std::tuple<SequenceData, uint64_t>(
            /*sequence data*/ {SequenceNumber(i), ChunkNumber(noChunks + ChunkNumber::INITIAL), true},
            /*ts*/ i));
    }

    std::mt19937 randomGenerator(42);
    std::shuffle(watermarkBarriers.begin(), watermarkBarriers.end(), randomGenerator);

    std::atomic<uint64_t> globalUpdateCounter = 0;
    while (globalUpdateCounter < watermarkBarriers.size())
    {
        const auto copyGlobalUpdateCounter = globalUpdateCounter.load();
        const auto missingUpdates = watermarkBarriers.size() - globalUpdateCounter;
        const auto updatesThisRound = std::min(missingUpdates, 1 + (rand() % averageUpdatesPerRound));
        const auto maxUpdatePos = copyGlobalUpdateCounter + updatesThisRound;

        std::vector<std::thread> threads;
        threads.reserve(threadsCount);
        for (auto threadId = 0; threadId < threadsCount; threadId++)
        {
            threads.emplace_back(
                [&watermarkProcessor, this, &globalUpdateCounter, maxUpdatePos]()
                {
                    /// Emplacing the next updatesThisRound per thread
                    auto nextUpdatePos = 0_u64;
                    while ((nextUpdatePos = globalUpdateCounter++) < maxUpdatePos)
                    {
                        auto currentWatermarkBarrier = watermarkBarriers[nextUpdatePos];
                        const auto& seqDataToInsert = std::get<0>(currentWatermarkBarrier);
                        const auto& valueToInsert = std::get<1>(currentWatermarkBarrier);
                        watermarkProcessor.emplace(seqDataToInsert, valueToInsert);
                    }
                });
        }

        /// Waiting till all threads are finished emplacing for the current round
        for (auto& thread : threads)
        {
            thread.join();
        }

        /// It can happen that multiple threads write over the maxUpdatePos. Therefore, we have to set it back.
        globalUpdateCounter = maxUpdatePos;

        /// Emplacing in mock-up queue the same updates
        auto currentValueExpected = 0_u64;
        for (auto i = copyGlobalUpdateCounter; i < globalUpdateCounter; ++i)
        {
            auto currentWatermarkBarrier = watermarkBarriers[i];
            const auto& seqDataToInsert = std::get<0>(currentWatermarkBarrier);
            const auto& valueToInsert = std::get<1>(currentWatermarkBarrier);
            currentValueExpected = emplaceInMockupQueue(seqDataToInsert, valueToInsert);
        }
        const auto newWatermark = watermarkProcessor.getCurrentValue();
        ASSERT_EQ(newWatermark, currentValueExpected);
    }

    /// add the last remaining watermark, as a result we now apply all remaining watermarks.
    ASSERT_EQ(watermarkProcessor.getCurrentValue(), noSeqNumbers);
}

struct BufferMetaDataTest
{
    SequenceData sequenceData;
    Runtime::Timestamp timestamp;
};

TEST_F(NonBlockingMonotonicSeqQueueTest, simpleInsertionsWithSingleChunks)
{
    std::vector<BufferMetaDataTest> sequenceData = {
        BufferMetaDataTest{.sequenceData = {SequenceNumber(1), INITIAL_CHUNK_NUMBER, true}, .timestamp = Runtime::Timestamp(31)},
        BufferMetaDataTest{.sequenceData = {SequenceNumber(2), INITIAL_CHUNK_NUMBER, true}, .timestamp = Runtime::Timestamp(63)},
        BufferMetaDataTest{.sequenceData = {SequenceNumber(3), INITIAL_CHUNK_NUMBER, true}, .timestamp = Runtime::Timestamp(80)},
        BufferMetaDataTest{.sequenceData = {SequenceNumber(4), INITIAL_CHUNK_NUMBER, true}, .timestamp = Runtime::Timestamp(99)},
    };

    auto watermarkProcessor = Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>();

    /// Inserting the first sequence ---> current value should be the timestamp of the first sequence
    watermarkProcessor.emplace(sequenceData[0].sequenceData, sequenceData[0].timestamp.getRawValue());
    EXPECT_EQ(watermarkProcessor.getCurrentValue(), sequenceData[0].timestamp.getRawValue());

    /// Inserting the second sequence ---> current value should be the timestamp of the second sequence
    watermarkProcessor.emplace(sequenceData[1].sequenceData, sequenceData[1].timestamp.getRawValue());
    EXPECT_EQ(watermarkProcessor.getCurrentValue(), sequenceData[1].timestamp.getRawValue());

    /// Inserting the fourth sequence ---> current value should be the timestamp of the second sequence, as we have not inserted the third sequence
    watermarkProcessor.emplace(sequenceData[3].sequenceData, sequenceData[3].timestamp.getRawValue());
    EXPECT_EQ(watermarkProcessor.getCurrentValue(), sequenceData[1].timestamp.getRawValue());

    /// Inserting the third sequence ---> current value should be the timestamp of the fourth sequence, as we have inserted all four sequences
    watermarkProcessor.emplace(sequenceData[2].sequenceData, sequenceData[2].timestamp.getRawValue());
    EXPECT_EQ(watermarkProcessor.getCurrentValue(), sequenceData[3].timestamp.getRawValue());
}

}
