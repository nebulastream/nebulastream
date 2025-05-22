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
#include <latch>
#include <mutex>
#include <random>
#include <ranges>
#include <thread>
#include <unordered_set>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <SequenceShredder.hpp>

class StreamingMultiThreaderAutomatedSequenceShredderTest : public ::testing::Test
{
private:
    using SequenceShredder = NES::InputFormatters::SequenceShredder;

public:
    void SetUp() override { NES_INFO("Setting up StreamingMultiThreaderAutomatedSequenceShredderTest."); }

    void TearDown() override { NES_INFO("Tear down StreamingMultiThreaderAutomatedSequenceShredderTest class."); }

    template <size_t NUM_THREADS>
    class TestThreadPool
    {
        static constexpr size_t INITIAL_NUM_BITMAPS = 16;

    public:
        TestThreadPool(
            const SequenceShredder::SequenceNumberType upperBound, const std::optional<SequenceShredder::SequenceNumberType> fixedSeed)
            : sequenceShredder(SequenceShredder{1, INITIAL_NUM_BITMAPS}), currentSequenceNumber(1), completionLatch(NUM_THREADS)
        {
            for (size_t i = 0; i < NUM_THREADS; ++i)
            {
                std::mt19937_64 sequenceNumberGen;
                std::bernoulli_distribution boolDistribution{0.5};
                if (fixedSeed.has_value())
                {
                    sequenceNumberGen = std::mt19937_64{fixedSeed.value()};
                }
                else
                {
                    std::random_device rd;
                    std::mt19937_64 seedGen{rd()};
                    std::uniform_int_distribution<size_t> dis{0, SIZE_MAX};
                    const SequenceShredder::SequenceNumberType randomSeed = dis(seedGen);
                    sequenceNumberGen = std::mt19937_64{randomSeed};
                    NES_DEBUG("Initializing StreamingSequenceNumberGenerator with random seed: {}", randomSeed);
                }
                threads.at(i) = std::jthread(
                    [this, i, upperBound, sequenceNumberGen = std::move(sequenceNumberGen), boolDistribution = std::move(boolDistribution)]
                    { threadFunction(i, upperBound, sequenceNumberGen, boolDistribution); });
            }
        }

        /// Check if at least one thread is still active
        void waitForCompletion() const { completionLatch.wait(); }

        [[nodiscard]] SequenceShredder::SequenceNumberType getCheckSum() const
        {
            SequenceShredder::SequenceNumberType globalCheckSum = 1;
            for (size_t i = 0; i < NUM_THREADS; ++i)
            {
                globalCheckSum += threadLocalCheckSum.at(i);
            }
            return globalCheckSum;
        }

    private:
        SequenceShredder sequenceShredder;
        std::atomic<size_t> currentSequenceNumber;
        std::atomic<SequenceShredder::SequenceNumberType> indexOfLastDetectedTupleDelimiter;
        std::array<std::jthread, NUM_THREADS> threads;
        std::array<SequenceShredder::SequenceNumberType, NUM_THREADS> threadLocalCheckSum;
        std::array<bool, NUM_THREADS> threadIsActive;
        std::latch completionLatch;
        std::mutex sequenceShredderMutex;

        void threadFunction(
            size_t threadIdx, const size_t upperBound, std::mt19937_64 sequenceNumberGen, std::bernoulli_distribution boolDistribution)
        {
            threadLocalCheckSum.at(threadIdx) = 0;

            /// Each thread gets and processes new sequence numbers, until they reach the upper bound.
            for (auto threadLocalSequenceNumber = currentSequenceNumber.fetch_add(1); threadLocalSequenceNumber <= upperBound;
                 threadLocalSequenceNumber = currentSequenceNumber.fetch_add(1))
            {
                /// Force a tuple delimiter for the first and the last sequence number, to guarantee the check sum.
                const bool tupleDelimiter
                    = boolDistribution(sequenceNumberGen) or (threadLocalSequenceNumber == 1) or (threadLocalSequenceNumber == upperBound);
                auto tupleDelimiterIndex = indexOfLastDetectedTupleDelimiter.load();

                while (tupleDelimiterIndex > threadLocalSequenceNumber
                       and not(indexOfLastDetectedTupleDelimiter.compare_exchange_weak(tupleDelimiterIndex, threadLocalSequenceNumber)))
                {
                    /// CAS loop implementing std::atomic_max
                }

                /// If the sequence number is not in range yet, busy wait until it is.
                while (not sequenceShredder.isInRange(threadLocalSequenceNumber))
                {
                }

                const auto dummyStagedBuffer = SequenceShredder::StagedBuffer{
                    .buffer = NES::Memory::TupleBuffer{},
                    .sizeOfBufferInBytes = threadLocalSequenceNumber,
                    .offsetOfFirstTupleDelimiter = 0,
                    .offsetOfLastTupleDelimiter = 0};
                if (tupleDelimiter)
                {
                    const auto stagedBuffers
                        = sequenceShredder.processSequenceNumber<true>(dummyStagedBuffer, threadLocalSequenceNumber).stagedBuffers;
                    if (stagedBuffers.size() > 1)
                    {
                        const auto spanStart = stagedBuffers.front().sizeOfBufferInBytes;
                        const auto spanEnd = stagedBuffers.back().sizeOfBufferInBytes;
                        const auto localCheckSum = spanEnd - spanStart;
                        threadLocalCheckSum.at(threadIdx) += localCheckSum;
                    }
                }
                else
                {
                    const auto stagedBuffers
                        = sequenceShredder.processSequenceNumber<false>(dummyStagedBuffer, threadLocalSequenceNumber).stagedBuffers;
                    if (stagedBuffers.size() > 1)
                    {
                        const auto spanStart = stagedBuffers.front().sizeOfBufferInBytes;
                        const auto spanEnd = stagedBuffers.back().sizeOfBufferInBytes;
                        const auto localCheckSum = spanEnd - spanStart;
                        threadLocalCheckSum.at(threadIdx) += localCheckSum;
                    }
                }
            }
            completionLatch.count_down();
        }
    };

    template <size_t NUM_THREADS>
    static void
    executeTest(const SequenceShredder::SequenceNumberType upperBound, const std::optional<SequenceShredder::SequenceNumberType> fixedSeed)
    {
        TestThreadPool testThreadPool = TestThreadPool<NUM_THREADS>(upperBound, fixedSeed);
        testThreadPool.waitForCompletion();
        const auto checkSum = testThreadPool.getCheckSum();
        ASSERT_EQ(checkSum, upperBound);
    }
};

TEST_F(StreamingMultiThreaderAutomatedSequenceShredderTest, multiThreadedExhaustiveTest)
{
    constexpr size_t numIterations = 1;
    for (size_t iteration = 0; iteration < numIterations; ++iteration)
    {
        executeTest<16>(100000, std::nullopt);
    }
}
