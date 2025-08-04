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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <latch>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <utility>
#include <vector>

#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>

#include <ErrorHandling.hpp>

/// Tests whether the SequenceShredder correctly finds spanning tuples given random orders of sequence numbers and random occurrences of
/// tuple delimiters in the buffers that belong to the sequence numbers.
/// Uses multiple threads that call the SequenceShredder to determine spanning tuples. Each thread randomly (seeded) determines whether its current
/// request has a tuple delimiter or not, calls the 'processSequenceNumber' function of the SequenceShredder and tracks the resulting spanning tuples.
/// We check whether the range of all produces sequence numbers matches the expected range.
class ConcurrentSynchronizationTest : public ::testing::Test
{
    using SequenceShredder = NES::InputFormatters::SequenceShredder;

public:
    void SetUp() override { NES_INFO("Setting up ConcurrentSynchronizationTest."); }

    void TearDown() override { NES_INFO("Tear down ConcurrentSynchronizationTest class."); }

    template <size_t NUM_THREADS>
    class TestThreadPool
    {
        static constexpr size_t INITIAL_NUM_BITMAPS = 16;

    public:
        TestThreadPool(
            const NES::InputFormatters::SequenceNumberType upperBound,
            const std::optional<NES::InputFormatters::SequenceNumberType> fixedSeed,
            NES::Memory::TupleBuffer dummyBuffer)
            : sequenceShredder(SequenceShredder{}), currentSequenceNumber(1), completionLatch(NUM_THREADS)
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
                    const NES::InputFormatters::SequenceNumberType randomSeed = dis(seedGen);
                    sequenceNumberGen = std::mt19937_64{randomSeed};
                    NES_DEBUG("Initializing StreamingSequenceNumberGenerator with random seed: {}", randomSeed);
                }
                threads.at(i)
                    = std::jthread([this,
                                    i,
                                    upperBound,
                                    sequenceNumberGen = std::move(sequenceNumberGen),
                                    boolDistribution = std::move(boolDistribution),
                                    dummyBuffer] { threadFunction(i, upperBound, sequenceNumberGen, boolDistribution, dummyBuffer); });
            }
        }
        /// Check if at least one thread is still active
        void waitForCompletion() const { completionLatch.wait(); }

        [[nodiscard]] NES::InputFormatters::SequenceNumberType getCheckSum() const
        {
            NES::InputFormatters::SequenceNumberType globalCheckSum = 0;
            for (size_t i = 0; i < NUM_THREADS; ++i)
            {
                globalCheckSum += threadLocalCheckSum.at(i);
            }
            return globalCheckSum;
        }

    private:
        SequenceShredder sequenceShredder;
        std::atomic<size_t> currentSequenceNumber;
        std::atomic<NES::InputFormatters::SequenceNumberType> indexOfLastDetectedTupleDelimiter;
        std::array<std::jthread, NUM_THREADS> threads;
        std::array<NES::InputFormatters::SequenceNumberType, NUM_THREADS> threadLocalCheckSum;
        std::array<bool, NUM_THREADS> threadIsActive;
        std::latch completionLatch;
        std::mutex sequenceShredderMutex;

        void threadFunction(
            size_t threadIdx,
            const size_t upperBound,
            std::mt19937_64 sequenceNumberGen,
            std::bernoulli_distribution boolDistribution,
            NES::Memory::TupleBuffer dummyBuffer)
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

                /// Since all threads copy the same reference, all copies of that reference point to the same buffer control block
                /// Thus, we can't set the sequence number in that control block. Instead, we exploit the 'offset of last tuple delimiter',
                /// of the StagedBuffer, which we create during each iteration and which is not manipulated by other threads
                const auto dummyStagedBuffer = NES::InputFormatters::StagedBuffer{
                    NES::InputFormatters::RawTupleBuffer{dummyBuffer},
                    threadLocalSequenceNumber,
                    0,
                    static_cast<uint32_t>(threadLocalSequenceNumber)};

                if (tupleDelimiter)
                {
                    NES::InputFormatters::SequenceShredderResult result
                        = sequenceShredder.findSTsWithDelimiter(dummyStagedBuffer, threadLocalSequenceNumber);
                    while (not result.isInRange)
                    {
                        result = sequenceShredder.findSTsWithDelimiter(dummyStagedBuffer, threadLocalSequenceNumber);
                    }
                    if (result.spanningBuffers.size() > 1)
                    {
                        /// The 'offset of last tuple delimiter' contains the sequence number (see comment above)
                        const auto spanStart = result.spanningBuffers.front().getOffsetOfLastTupleDelimiter();
                        const auto spanEnd = result.spanningBuffers.back().getOffsetOfLastTupleDelimiter();
                        const auto localCheckSum = spanEnd - spanStart;
                        threadLocalCheckSum.at(threadIdx) += localCheckSum;
                    }
                }
                else
                {
                    NES::InputFormatters::SequenceShredderResult result
                        = sequenceShredder.findSTsWithoutDelimiter(dummyStagedBuffer, threadLocalSequenceNumber);
                    while (not result.isInRange)
                    {
                        result = sequenceShredder.findSTsWithoutDelimiter(dummyStagedBuffer, threadLocalSequenceNumber);
                    }
                    if (result.spanningBuffers.size() > 1)
                    {
                        /// The 'offset of last tuple delimiter' contains the sequence number (see comment above)
                        const auto spanStart = result.spanningBuffers.front().getOffsetOfLastTupleDelimiter();
                        const auto spanEnd = result.spanningBuffers.back().getOffsetOfLastTupleDelimiter();
                        const auto localCheckSum = spanEnd - spanStart;
                        threadLocalCheckSum.at(threadIdx) += localCheckSum;
                    }
                }
            }
            completionLatch.count_down();
        }
    };

    template <size_t NUM_THREADS>
    static void executeTest(const uint32_t upperBound, const std::optional<NES::InputFormatters::SequenceNumberType> fixedSeed)
    {
        PRECONDITION(upperBound <= std::numeric_limits<uint32_t>::max(), "Not supporting values larger than 4294967295");
        /// To avoid (future) errors by creating a TupleBuffer without a valid control block, we create a single valid (dummy) tuple buffer
        /// All threads share the reference to that buffer throughout this test
        const auto testBufferManager = NES::Memory::BufferManager::create(1, 1);
        const auto dummyBuffer = testBufferManager->getBufferBlocking();
        TestThreadPool testThreadPool = TestThreadPool<NUM_THREADS>(upperBound, fixedSeed, dummyBuffer);
        testThreadPool.waitForCompletion();
        const auto checkSum = testThreadPool.getCheckSum();
        ASSERT_EQ(checkSum, upperBound);
    }
};

TEST_F(ConcurrentSynchronizationTest, multiThreadedExhaustiveTest)
{
    constexpr size_t numIterations = 1;
    for (size_t iteration = 0; iteration < numIterations; ++iteration)
    {
        executeTest<16>(1000000, std::nullopt);
    }
}
