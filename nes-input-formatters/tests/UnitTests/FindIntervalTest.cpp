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
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>

struct Interval
{
    NES::SequenceNumber::Underlying lo;
    NES::SequenceNumber::Underlying hi;
};
using SpanningTupleBuffers = std::vector<Interval>;

class SequenceShredder
{
public:
    using SequenceNumberType = NES::SequenceNumber::Underlying;
    static constexpr SequenceNumberType INVALID_SN = NES::INVALID<NES::SequenceNumber>.getRawValue();
    static constexpr size_t WIDTH_OF_RB = 1024;
    static constexpr size_t ABA_TAG_MASK = 18446744069414584320ULL; /// lower 32 bits

private:
    static uint64_t createInitialAtomicState(const uint64_t currentTail, const bool hasTupleDelimiter)
    {
        uint64_t initialAtomicState = 0;
        const uint32_t iterationABATag = currentTail / WIDTH_OF_RB;
        initialAtomicState |= iterationABATag;
        constexpr uint64_t hasBeenIndexedBit = 1;
        const uint64_t hasTupleDelimiterBit = hasTupleDelimiter;
        initialAtomicState |= (hasBeenIndexedBit << 32);
        initialAtomicState |= (hasTupleDelimiterBit << 33);
        /// UsedForIntervalStart = 0
        /// UncertainDelimiter = 0
        /// UncertainEscapeSequence = 0
        /// NextBufferWithDelimiterLeading = 0
        /// NextBufferWithDelimiterTrailing = 0
        return initialAtomicState;
    }

    static bool asHasTupleDelimiter(const uint64_t atomicState)
    {
        constexpr uint64_t hasTupleDelimiterMask = 8589934592ULL;
        return static_cast<bool>(atomicState & hasTupleDelimiterMask);
    }

    static bool asHasBeenIndexed(const uint64_t atomicState)
    {
        constexpr uint64_t hasBeenIndexed = 4294967296ULL;
        return static_cast<bool>(atomicState & hasBeenIndexed);
    }

    static bool asClaimBuffer(std::atomic<uint64_t>& atomicState, uint64_t expected)
    {
        const uint64_t usedForIntervalStart = expected | 17179869184ULL;
        const auto result = atomicState.compare_exchange_strong(expected, usedForIntervalStart);
        return result; /// should only be true if ABATag is correct AND 'sudeForIntervalStart' bit was not set yet
        // return expected != expected;
    }

public:
    /// [1-8]: SSMetadata
    /// [9-32]: LeadingSTUse
    /// [33-56]: TrailingSTUse
    /// [57-60]: offsetOfFirstTupleDelimiter
    /// [61-64]: offsetOfLastTupleDelimiter
    struct SSMetaData
    {
        SSMetaData() = default;
        SSMetaData(const NES::InputFormatters::StagedBuffer& buffer, const uint64_t currentTail, const bool hasTupleDelimiter)
            : atomicState(createInitialAtomicState(currentTail, hasTupleDelimiter))
            , leadingSTUse(buffer.getRawTupleBuffer().getRawBuffer())
            , trailingSTUse(buffer.getRawTupleBuffer().getRawBuffer())
            , offsetOfFirstTupleDelimiter(buffer.getLeadingBytes().size())
            , offsetOfLastTupleDelimiter(buffer.getTrailingBytes(1).size())
        {
        }

        void copyFrom(const NES::InputFormatters::StagedBuffer& buffer, const uint64_t currentTail, const bool hasTupleDelimiter)
        {
            this->atomicState = createInitialAtomicState(currentTail, hasTupleDelimiter);
            this->leadingSTUse = buffer.getRawTupleBuffer().getRawBuffer();
            this->trailingSTUse = buffer.getRawTupleBuffer().getRawBuffer();
            this->offsetOfFirstTupleDelimiter = 0;
            this->offsetOfLastTupleDelimiter = 0;
        }
        /// [1-32] : Tag (against ABA)
        /// [57-64]: Metadata bits
        ///     [58]: HasBeenIndexed
        ///     [59]: HasTupleDelimiter
        ///     [60]: UsedForIntervalStart
        ///     [61]: UncertainDelimiter
        ///     [62]: UncertainEscapeSequence
        ///     [63]: ?
        ///     [64]: ?
        /// [33-44]: Next buffer with TupleDelimiter (leading)
        /// [45-56]: Next buffer with TupleDelimiter (trailing)
        std::atomic<uint64_t> atomicState;
        NES::Memory::TupleBuffer leadingSTUse;
        NES::Memory::TupleBuffer trailingSTUse;
        uint32_t offsetOfFirstTupleDelimiter{0};
        uint32_t offsetOfLastTupleDelimiter{0};
    };
    static_assert((sizeof(SSMetaData) == 64));

    SequenceShredder() : tailBytes(std::vector<uint8_t>(WIDTH_OF_RB * 2)), ringBuffer(WIDTH_OF_RB), tail(0) { }

    bool isInRange(const SequenceNumberType sequenceNumber)
    {
        /// If the current SN is out of range, recalculate the tail and check one more time
        if (auto currentTail = tail.load(); (sequenceNumber + 1) > (currentTail + WIDTH_OF_RB))
        {
            size_t progressCounter = 0;
            while (progressCounter < WIDTH_OF_RB
                   and tailBytes[(currentTail + progressCounter) % WIDTH_OF_RB]
                       == (((currentTail + progressCounter) / WIDTH_OF_RB) + 1) % 2)
            {
                ++progressCounter;
            }
            const auto newTail = currentTail + progressCounter;
            return tail.compare_exchange_strong(currentTail, newTail);
        }
        return true;
    }

    template <bool HasTupleDelimiter>
    SpanningTupleBuffers
    processSequenceNumber(const NES::InputFormatters::StagedBuffer& stagedBuffer, const SequenceNumberType sequenceNumber)
    {
        // Todo: just focus on implementing Interval-detection logic
        // - start traversing ringBuffer towards leading direction
        // - start traversing ringBuffer towards trailing direction
        // - handle spanning tuples if necessary
        //      - remove uses and construct SpanningTupleBuffers
        //      - set tail bits

        const auto snIndex = sequenceNumber % WIDTH_OF_RB;
        const auto currentTail = tail.load(); // Get rid of additional load? <-- could return from 'isInRange' and then require here
        const uint32_t tailIterationABATag = currentTail / WIDTH_OF_RB;
        ringBuffer.at(snIndex).copyFrom(stagedBuffer, currentTail, HasTupleDelimiter);

        // Todo: traversal (start without optimization)

        if constexpr (HasTupleDelimiter)
        {
            SequenceNumberType leadingSpanningTupleStartIdx = INVALID_SN;
            SequenceNumberType trailingSpanningTupleStartIdx = INVALID_SN;
            SequenceNumberType loOffset = 1;
            while ((snIndex - loOffset) > 0 and loOffset < WIDTH_OF_RB)
            {
                const auto nextLoBufferIdx = (snIndex - loOffset) % WIDTH_OF_RB;
                const auto nextBufferAtomicState = ringBuffer.at(nextLoBufferIdx).atomicState.load();

                if (asHasTupleDelimiter(nextBufferAtomicState))
                {
                    /// Zero out lower 32 bits, then set new tag (if tags are equal, there are no changes and the CAS succeeds)
                    const auto nextBufferStateTagged = (nextBufferAtomicState & ABA_TAG_MASK) | tailIterationABATag;
                    if (asClaimBuffer(ringBuffer.at(nextLoBufferIdx).atomicState, nextBufferStateTagged))
                    {
                        /// next buffer had tuple delimiter and was not claimed yet
                        /// the current thread processes a buffer with a tuple delimiter and can thus claim the spanning tuple
                        leadingSpanningTupleStartIdx = nextLoBufferIdx;
                    }
                    break;
                }
                /// the buffer has no tuple delimiter, check if we saw it already
                if (not asHasBeenIndexed(nextBufferAtomicState))
                {
                    /// We found a buffer that we haven't seen yet, thus we can't continue
                    break;
                }
                ++loOffset;
            }
            SequenceNumberType hiOffset = 1;
            while (hiOffset < WIDTH_OF_RB)
            {
                const auto nextHiBufferIdx = (snIndex + hiOffset) % WIDTH_OF_RB;
                const auto nextBufferAtomicState = ringBuffer.at(nextHiBufferIdx).atomicState.load();

                if (asHasTupleDelimiter(nextBufferAtomicState))
                {
                    /// Zero out lower 32 bits, then set new tag (if tags are equal, there are no changes and the CAS succeeds)
                    const auto nextBufferStateTagged = (nextBufferAtomicState & ABA_TAG_MASK) | tailIterationABATag;
                    if (asClaimBuffer(ringBuffer.at(snIndex).atomicState, nextBufferStateTagged))
                    {
                        /// next buffer had tuple delimiter and was not claimed yet
                        /// the current thread processes a buffer with a tuple delimiter and can thus claim the spanning tuple
                        trailingSpanningTupleStartIdx = nextHiBufferIdx;
                    }
                    break;
                }
                /// the buffer has no tuple delimiter, check if we saw it already
                if (asHasBeenIndexed(nextBufferAtomicState))
                {
                    ++hiOffset;
                }
                /// We found a buffer that we haven't seen yet, thus we can't continue
                break;
            }
            // Todo: handle spanning tuples (for now simply return Intervals)
            // Todo: handle tail calculation
            if (leadingSpanningTupleStartIdx != INVALID_SN and trailingSpanningTupleStartIdx != INVALID_SN)
            {
                return SpanningTupleBuffers{Interval{leadingSpanningTupleStartIdx, sequenceNumber}, Interval{sequenceNumber, trailingSpanningTupleStartIdx}};
            }
            if (leadingSpanningTupleStartIdx != INVALID_SN and trailingSpanningTupleStartIdx == INVALID_SN)
            {
                return SpanningTupleBuffers{Interval{leadingSpanningTupleStartIdx, sequenceNumber}};
            }
            if (leadingSpanningTupleStartIdx == INVALID_SN and trailingSpanningTupleStartIdx != INVALID_SN)
            {
                return SpanningTupleBuffers{Interval{sequenceNumber, trailingSpanningTupleStartIdx}};
            }
            return {};
        } else
        {
            SequenceNumberType leadingSpanningTupleStartIdx = INVALID_SN;
            SequenceNumberType trailingSpanningTupleStartIdx = INVALID_SN;

            SequenceNumberType loOffset = 1;
            while ((snIndex - loOffset) > 0 and loOffset < WIDTH_OF_RB)
            {
                const auto nextLoBufferIdx = (snIndex - loOffset) % WIDTH_OF_RB;
                const auto nextBufferAtomicState = ringBuffer.at(nextLoBufferIdx).atomicState.load();

                if (asHasTupleDelimiter(nextBufferAtomicState))
                {
                    /// Zero out lower 32 bits, then set new tag (if tags are equal, there are no changes and the CAS succeeds)
                    const auto nextBufferStateTagged = (nextBufferAtomicState & ABA_TAG_MASK) | tailIterationABATag;
                    if (asClaimBuffer(ringBuffer.at(nextLoBufferIdx).atomicState, nextBufferStateTagged))
                    {
                        /// next buffer had tuple delimiter and was not claimed yet
                        /// the current thread processes a buffer with a tuple delimiter and can thus claim the spanning tuple
                        leadingSpanningTupleStartIdx = nextLoBufferIdx;
                    }
                    break;
                }
                /// the buffer has no tuple delimiter, check if we saw it already
                if (not asHasBeenIndexed(nextBufferAtomicState))
                {
                    /// We found a buffer that we haven't seen yet, thus we can't continue
                    break;
                }
                ++loOffset;
            }
            if (leadingSpanningTupleStartIdx == INVALID_SN)
            {
                return {};
            }
            SequenceNumberType hiOffset = 1;
            while (hiOffset < WIDTH_OF_RB)
            {
                const auto nextHiBufferIdx = (snIndex + hiOffset) % WIDTH_OF_RB;
                const auto nextBufferAtomicState = ringBuffer.at(nextHiBufferIdx).atomicState.load();

                if (asHasTupleDelimiter(nextBufferAtomicState))
                {
                    /// Zero out lower 32 bits, then set new tag (if tags are equal, there are no changes and the CAS succeeds)
                    const auto nextBufferStateTagged = (nextBufferAtomicState & ABA_TAG_MASK) | tailIterationABATag;
                    if (asClaimBuffer(ringBuffer.at(snIndex).atomicState, nextBufferStateTagged))
                    {
                        /// next buffer had tuple delimiter and was not claimed yet
                        /// the current thread processes a buffer with a tuple delimiter and can thus claim the spanning tuple
                        trailingSpanningTupleStartIdx = nextHiBufferIdx;
                    }
                    break;
                }
                /// the buffer has no tuple delimiter, check if we saw it already
                if (asHasBeenIndexed(nextBufferAtomicState))
                {
                    ++hiOffset;
                }
                /// We found a buffer that we haven't seen yet, thus we can't continue
                break;
            }
            // Todo: handle spanning tuples (for now simply return Intervals)
            // Todo: handle tail calculation
            if (trailingSpanningTupleStartIdx != INVALID_SN)
            {
                return {Interval{leadingSpanningTupleStartIdx, trailingSpanningTupleStartIdx}};
            }
            return {};
        }
    }

private:
    /// tailBytes are twice as big as the size of the ring buffer
    /// each SequenceNumber is represented by two cells in the tailBytes, each of the two cells represents one possible use (leading/trailing)
    std::vector<uint8_t> tailBytes;
    std::vector<SSMetaData> ringBuffer;
    std::atomic<uint64_t> tail;
};

class LocklessSequenceShredderTest : public ::testing::Test
{
public:
    void SetUp() override { NES_INFO("Setting up LocklessSequenceShredderTest."); }

    void TearDown() override { NES_INFO("Tear down LocklessSequenceShredderTest class."); }

    template <size_t NUM_THREADS>
    class TestThreadPool
    {
    public:
        TestThreadPool(
            const SequenceShredder::SequenceNumberType upperBound, const std::optional<SequenceShredder::SequenceNumberType> fixedSeed)
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

                const auto dummyStagedBuffer
                    = NES::InputFormatters::StagedBuffer{NES::InputFormatters::RawTupleBuffer{}, threadLocalSequenceNumber, 0, 0};
                if (tupleDelimiter)
                {
                    const auto stagedBuffers
                        = sequenceShredder.processSequenceNumber<true>(dummyStagedBuffer, threadLocalSequenceNumber);
                    if (stagedBuffers.size() > 0)
                    {
                        const auto spanStart = stagedBuffers.front().lo;
                        const auto spanEnd = stagedBuffers.back().hi;
                        std::cout << fmt::format("[{}-{}]\n", spanStart, spanEnd) << std::flush;
                        const auto localCheckSum = spanEnd - spanStart;
                        threadLocalCheckSum.at(threadIdx) += localCheckSum;
                    }
                }
                else
                {
                    const auto stagedBuffers
                        = sequenceShredder.processSequenceNumber<false>(dummyStagedBuffer, threadLocalSequenceNumber);
                    if (stagedBuffers.size() > 0)
                    {
                        const auto spanStart = stagedBuffers.front().lo;
                        const auto spanEnd = stagedBuffers.back().hi;
                        std::cout << fmt::format("[{}-{}]\n", spanStart, spanEnd) << std::flush;
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

TEST_F(LocklessSequenceShredderTest, multiThreadedExhaustiveTest)
{
    // fmt::println("{}", sizeof(SequenceShredder::SSMetaData));
    constexpr size_t numIterations = 1;
    for (size_t iteration = 0; iteration < numIterations; ++iteration)
    {
        // Todo: find race condition (already occurs with 2 threads)
        executeTest<2>(100, 1);
        // executeTest<16>(100000, std::nullopt);
    }
}
