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
#include <latch>
#include <random>
#include <ranges>
#include <thread>
#include <unordered_set>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <gtest/gtest.h>
#include <SequenceShredder.hpp>

class StreamingMultiThreaderAutomatedSequenceShredderTest : public ::testing::Test {
private:
    using SequenceShredder = NES::InputFormatters::SequenceShredder;
public:
    struct GeneratorReturnType {
        SequenceShredder::SequenceNumberType sequenceNumber;
        bool hasTupleDelimiter;
        bool generatedNewSequence;
        bool reachedUpperBound;
    };

    void SetUp() override {
        std::cout << "Setting up StreamingMultiThreaderAutomatedSequenceShredderTest.\n";
    }

    void TearDown() override {
        std::cout << "Tear down StreamingMultiThreaderAutomatedSequenceShredderTest class.\n";
    }

    template <size_t NUM_THREADS>
    class TestThreadPool {
        static constexpr size_t INITIAL_NUM_BITMAPS = 16;
      public:
        TestThreadPool(
              const SequenceShredder::SequenceNumberType upperBound, std::optional<SequenceShredder::SequenceNumberType> fixedSeed)
              : sequenceShredder(SequenceShredder{1, INITIAL_NUM_BITMAPS}), currentSequenceNumber(1), completionLatch(NUM_THREADS)
        {
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
              std::cout << "Initializing StreamingSequenceNumberGenerator with random seed: " << randomSeed << std::endl;
            }
            for (size_t i = 0; i < NUM_THREADS; ++i)
            {
              threads[i] = std::jthread([this, i, upperBound] { thread_function(i, upperBound); });
            }
        }
          /// Check if at least one thread is still active
        void waitForCompletion() {
            // Wait for all threads to complete
            completionLatch.wait();
        }

        SequenceShredder::SequenceNumberType getTail() {
            return sequenceShredder.getTail();
        }

        SequenceShredder::SequenceNumberType getCheckSum() {
            SequenceShredder::SequenceNumberType globalCheckSum = 1;
            for (size_t i = 0; i < NUM_THREADS; ++i) {
                globalCheckSum += threadLocalCheckSum[i];
                std::cout << "local checksum for thread: " << i << " is: " << threadLocalCheckSum[i] << std::endl;
            }
            return globalCheckSum;
        }
      private:
        SequenceShredder sequenceShredder;
        std::atomic<size_t> currentSequenceNumber;
        std::atomic<SequenceShredder::SequenceNumberType> indexOfLastDetectedTupleDelimiter; //Todo: rename to highlight index character
        std::array<std::jthread, NUM_THREADS> threads;
        std::array<SequenceShredder::SequenceNumberType, NUM_THREADS> threadLocalCheckSum;
        std::array<bool, NUM_THREADS> threadIsActive;
        std::latch completionLatch;
        std::mutex sequenceShredderMutex;

        /// Randomization for tuple delimiters
        std::mt19937_64 sequenceNumberGen;
        std::bernoulli_distribution boolDistribution{0.5};

      private:
        void thread_function(size_t threadIdx, const size_t upperBound)
        {
            threadLocalCheckSum[threadIdx] = 0;
            // Todo: we must increase and get the currentSequenceNumber at the same time!

            for (auto threadLocalSequenceNumber = currentSequenceNumber.fetch_add(1); threadLocalSequenceNumber <= upperBound; threadLocalSequenceNumber = currentSequenceNumber.fetch_add(1))
            {
                /// Force a tuple delimiter for the first and the last sequence number, to guarantee the check sum.
                const bool tupleDelimiter = boolDistribution(sequenceNumberGen) or (threadLocalSequenceNumber == 1) or (threadLocalSequenceNumber == upperBound);
                auto tupleDelimiterIndex = indexOfLastDetectedTupleDelimiter.load();

                while (tupleDelimiterIndex > threadLocalSequenceNumber and not(indexOfLastDetectedTupleDelimiter.compare_exchange_weak(tupleDelimiterIndex, threadLocalSequenceNumber))) {
                    /// CAS loop implementing std::atomic_max
                }

                // If the sequence number is not in range yet, busy wait until it is.
                while (not sequenceShredder.isSequenceNumberInRange(threadLocalSequenceNumber)) {}

                const auto dummyStagedBuffer = SequenceShredder::StagedBuffer{
                    .buffer = NES::Memory::TupleBuffer{},
                    .sizeOfBufferInBytes = threadLocalSequenceNumber,
                    .offsetOfFirstTupleDelimiter = 0,
                    .offsetOfLastTupleDelimiter = 0};
                if (tupleDelimiter) {
                    const auto stagedBuffers = sequenceShredder.processSequenceNumber<true>(dummyStagedBuffer, threadLocalSequenceNumber).stagedBuffers;
                    if (stagedBuffers.size() > 1)
                    {
                        const auto spanStart = stagedBuffers.front().sizeOfBufferInBytes;
                        const auto spanEnd = stagedBuffers.back().sizeOfBufferInBytes;
                        const auto localCheckSum = spanEnd - spanStart;
                        threadLocalCheckSum.at(threadIdx) += localCheckSum;
                    }
                } else {
                    const auto stagedBuffers = sequenceShredder.processSequenceNumber<false>(dummyStagedBuffer, threadLocalSequenceNumber).stagedBuffers;
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
    static void executeTest(const SequenceShredder::SequenceNumberType upperBound, const std::optional<SequenceShredder::SequenceNumberType> fixedSeed) {
        TestThreadPool testThreadPool = TestThreadPool<NUM_THREADS>(upperBound, fixedSeed);
        testThreadPool.waitForCompletion();
        const auto checkSum = testThreadPool.getCheckSum();
        const auto expectedTail = upperBound / SequenceShredder::SIZE_OF_BITMAP_IN_BITS;
        ASSERT_EQ(testThreadPool.getTail(), expectedTail);
        ASSERT_EQ(checkSum, upperBound);
    }

};

// Todo: remove seed if no randomization
TEST_F(StreamingMultiThreaderAutomatedSequenceShredderTest, multiThreadedExhaustiveTest) {
    constexpr size_t NUM_ITERATIONS = 1;
    for (size_t iteration = 0; iteration < NUM_ITERATIONS; ++iteration) {
        std::cout << "Iteration " << iteration << std::endl;
        executeTest<32>(1000000, 4726316926588018883); //worked for (64 threads and 10 million upper bound) and (1 thread and 1 billion upper bound)
    }
}