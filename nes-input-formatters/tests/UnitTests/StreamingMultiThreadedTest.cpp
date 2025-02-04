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
#include "../../private/SequenceShredder.hpp"

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
      public:
        TestThreadPool(
              const SequenceShredder::SequenceNumberType upperBound, std::optional<SequenceShredder::SequenceNumberType> fixedSeed)
              : sequenceShredder(SequenceShredder{1}), currentSequenceNumber(1), completionLatch(NUM_THREADS)
        {
            if (fixedSeed.has_value())
            {
              sequenceNumberGen = std::mt19937_64{fixedSeed.value()};
              // std::cout << "Initializing StreamingSequenceNumberGenerator with fixed seed: " << fixedSeed.value() << std::endl;
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
        // std::shared_ptr<NES::Memory::BufferManager> testBufferManager;
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
                // bool tupleDelimiter = true;
                bool tupleDelimiter = boolDistribution(sequenceNumberGen) or (threadLocalSequenceNumber == 1) or (threadLocalSequenceNumber == upperBound);
                // bool tupleDelimiter = (threadLocalSequenceNumber % 5) == 0 or (threadLocalSequenceNumber == 1) or (threadLocalSequenceNumber == upperBound);
                auto tupleDelimiterIndex = indexOfLastDetectedTupleDelimiter.load();
                // const auto tailBitmap = (tupleDelimiterIndex - 1) / SequenceShredder::SIZE_OF_BITMAP_IN_BITS;
                // const auto targetBitmap = (threadLocalSequenceNumber - 1) / SequenceShredder::SIZE_OF_BITMAP_IN_BITS;
                /// If the current thread writes into the bitmap right behind the bitmap that contains the last known
                /// tuple delimiter, we set a tuple delimiter, to avoid spanning tuples that would wrap back into the tail
                // Todo: the thread might still be slow to set the bit, causing another thread to continue with sequence numbers
                //  from the tail bitmap
                // - it does guarantee that the current thread finds a SpanningTuple connecting at least to the previous tail,
                //   because we only increase the currentSequenceNumber, AFTER we processed it, so all sequence numbers up to
                //   the tail are reflected in the SequenceShredder bitmaps already.
                // if ((targetBitmap > tailBitmap) and ((targetBitmap - tailBitmap) == (numBitmaps - 1))) {
                //     tupleDelimiter = true;
                // }

                while (tupleDelimiterIndex > threadLocalSequenceNumber and not(indexOfLastDetectedTupleDelimiter.compare_exchange_weak(tupleDelimiterIndex, threadLocalSequenceNumber))) {
                    /// CAS loop implementing std::atomic_max
                }

                // If the sequence number is not in range yet, busy wait until it is.
                while (not sequenceShredder.isSequenceNumberInRange(threadLocalSequenceNumber)) {}

                const auto numUses = static_cast<uint8_t>(1 + static_cast<uint8_t>(tupleDelimiter));
                // auto testBuffer = testBufferManager->getBufferBlocking(); // Todo: is this call thread safe? otherwise lock or have thread-local bufferpools!
                // testBuffer.setSequenceNumber(NES::SequenceNumber(threadLocalSequenceNumber));
                // Todo: abuse sizeOfBufferInBytes as SequenceNumber?
                // const auto dummyBuffer = NES::Memory::TupleBuffer{};
                const auto dummyStagedBuffer = SequenceShredder::StagedBuffer{
                    .buffer = NES::Memory::TupleBuffer{},
                    .sizeOfBufferInBytes = threadLocalSequenceNumber,
                    .offsetOfFirstTupleDelimiter = 0,
                    .offsetOfLastTupleDelimiter = 0,
                    .uses = numUses};
                if (tupleDelimiter) {
                    // Todo: pass dummy buffer to sequence shredder
                    const auto stagedBuffers = sequenceShredder.processSequenceNumber<true>(dummyStagedBuffer, threadLocalSequenceNumber).stagedBuffers;
                    if (stagedBuffers.size() > 1)
                    {
                        const auto spanStart = stagedBuffers.front().sizeOfBufferInBytes;
                        const auto spanEnd = stagedBuffers.back().sizeOfBufferInBytes;
                        threadLocalCheckSum.at(threadIdx) += (spanEnd - spanStart);
                    }
                } else {
                    const auto stagedBuffers = sequenceShredder.processSequenceNumber<false>(dummyStagedBuffer, threadLocalSequenceNumber).stagedBuffers;
                    if (stagedBuffers.size() > 1)
                    {
                        const auto spanStart = stagedBuffers.begin()->sizeOfBufferInBytes;
                        const auto spanEnd = stagedBuffers.end()->sizeOfBufferInBytes;
                        threadLocalCheckSum.at(threadIdx) += (spanEnd - spanStart);
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
        executeTest<1>(10000000, 4726316926588018883); //worked for (64 threads and 10 million upper bound) and (1 thread and 1 billion upper bound)
    }
}