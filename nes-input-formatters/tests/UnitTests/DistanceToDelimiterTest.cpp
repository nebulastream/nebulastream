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
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <latch>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <ranges>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fmt/base.h>
#include <fmt/ranges.h>

#include <condition_variable>
#include <DataTypes/Schema.hpp>
#include <Util/Logger/Formatter.hpp>


#include <Util/Ranges.hpp>
#include "ErrorHandling.hpp"

class SSMetaData;

class AtomicBitmapState
{
    /// 33:   000000000000000000000000000000100000000000000000000000000000000
    static constexpr uint64_t hasTupleDelimiterBit = 4294967296ULL;
    /// 34:   000000000000000000000000000001000000000000000000000000000000000
    static constexpr uint64_t noTupleDelimiterBit = 8589934592ULL;
    /// 35:   000000000000000000000000000010000000000000000000000000000000000
    static constexpr uint64_t usedLeadingBufferBit = 17179869184ULL;
    /// 36:   000000000000000000000000000100000000000000000000000000000000000
    static constexpr uint64_t usedTrailingBufferBit = 34359738368ULL;
    /// 37:   000000000000000000000000001000000000000000000000000000000000000
    static constexpr uint64_t claimedSpanningTupleBit = 68719476736ULL;
    ///       000000000000000000000000000110000000000000000000000000000000000
    static constexpr uint64_t usedLeadingAndTrailingBufferBits = 51539607552ULL;
    /// Tag: 1, HasTupleDelimiter: True, NoTupleDelimiter: False, UsedLeading: True, UsedTrailing: False
    static constexpr uint64_t stateOfFirstEntry = 21474836481ULL;

    /// [1-32] : Iteration Tag:        protects against ABA and tells threads whether buffer is from the same iteration during ST search
    /// [33]   : HasTupleDelimiter:    when set, threads stop spanning tuple (ST) search, since the buffer represents a possible start/end
    /// [34]   : NoTupleDelimiter:     when set, threads continue spanning tuple search with next cell in ring buffer
    /// [35]   : UsedLeadingBuffer:    signals to threads that the leading buffer use was consumed already
    /// [36]   : UsedTrailingBuffer:   signals to threads that the trailing buffer use was consumed already
    /// [37]   : ClaimedSpanningTuple: signals to threads that the corresponding buffer was already claimed to start an ST by another thread
    class BitmapState
    {
    public:
        explicit BitmapState(const uint64_t state) : state(state) { };
        [[nodiscard]] uint32_t getABAItNo() const { return static_cast<uint32_t>(state); }
        [[nodiscard]] bool hasTupleDelimiter() const { return (state & hasTupleDelimiterBit) == hasTupleDelimiterBit; }
        [[nodiscard]] bool hasNoTupleDelimiter() const { return (state & noTupleDelimiterBit) == noTupleDelimiterBit; }
        [[nodiscard]] bool hasUsedLeadingBuffer() const { return (state & usedLeadingBufferBit) == usedLeadingBufferBit; }
        [[nodiscard]] bool hasUsedTrailingBuffer() const { return (state & usedTrailingBufferBit) == usedTrailingBufferBit; }
        [[nodiscard]] bool hasClaimedSpanningTuple() const { return (state & claimedSpanningTupleBit) == claimedSpanningTupleBit; }

        void updateLeading(const BitmapState other) { this->state |= (other.getBitmapState() & usedLeadingBufferBit); }

        void claimSpanningTuple() { this->state |= claimedSpanningTupleBit; }

    private:
        friend class AtomicBitmapState;
        [[nodiscard]] uint64_t getBitmapState() const { return state; }
        uint64_t state;
    };
    AtomicBitmapState() : state(51539607552) { };
    ~AtomicBitmapState() = default;

    AtomicBitmapState(const AtomicBitmapState&) = delete;
    AtomicBitmapState(const AtomicBitmapState&&) = delete;
    void operator=(const AtomicBitmapState& other) = delete;
    void operator=(const AtomicBitmapState&& other) = delete;

    [[nodiscard]] BitmapState getState() const { return BitmapState{this->state.load()}; }

    friend SSMetaData;
    bool tryClaimSpanningTuple(const uint32_t abaItNumber)
    {
        auto atomicFirstDelimiter = getState();
        auto desiredFirstDelimiter = atomicFirstDelimiter;
        desiredFirstDelimiter.claimSpanningTuple();

        bool claimedSpanningTuple = false;
        while (atomicFirstDelimiter.getABAItNo() == abaItNumber and not atomicFirstDelimiter.hasClaimedSpanningTuple()
               and not claimedSpanningTuple)
        {
            desiredFirstDelimiter.updateLeading(atomicFirstDelimiter);
            claimedSpanningTuple = this->state.compare_exchange_weak(atomicFirstDelimiter.state, desiredFirstDelimiter.state);
        }
        return claimedSpanningTuple;
    }

    void setStateOfFirstEntry()
    {
        this->state = stateOfFirstEntry;
    }
    void setHasTupleDelimiterState(const size_t abaItNumber)
    {
        this->state = (hasTupleDelimiterBit | abaItNumber);
    }
    void setNoTupleDelimiterState(const size_t abaItNumber)
    {
        this->state = (noTupleDelimiterBit | abaItNumber);
    }

    [[nodiscard]] uint32_t getABAItNo() const
    {
        /// The first 32 bits are the ABA number
        return static_cast<uint32_t>(state.load());
    }

    void setUsedLeadingBuffer() { this->state |= usedLeadingBufferBit; }
    void setUsedTrailingBuffer() { this->state |= usedTrailingBufferBit; }
    void setUsedLeadingAndTrailingBuffer() { this->state |= usedTrailingBufferBit; }


    friend std::ostream& operator<<(std::ostream& os, const AtomicBitmapState& atomicBitmapState)
    {
        const auto loadedBitmap = atomicBitmapState.getState();
        return os << fmt::format(
                   "[{}-{}-{}-{}-{}]",
                   loadedBitmap.hasTupleDelimiter(),
                   loadedBitmap.hasNoTupleDelimiter(),
                   loadedBitmap.getABAItNo(),
                   loadedBitmap.hasUsedLeadingBuffer(),
                   loadedBitmap.hasUsedTrailingBuffer(),
                   loadedBitmap.hasClaimedSpanningTuple());
    }

    std::atomic<uint64_t> state;
};

struct Interval
{
    uint32_t lo;
    uint32_t hi;

    bool operator<(const Interval& other) const { return this->lo < other.lo; }

    friend std::ostream& operator<<(std::ostream& os, const Interval& interval)
    {
        return os << '[' << interval.lo << ',' << interval.hi << ']';
    }
};

class SSMetaData
{
public:
    SSMetaData() : leadingBufferRefCount(1), trailingBufferRefCount(1) {};

    template <bool HasTupleDelimiter>
    bool isInRange(const size_t abaItNumber)
    {
        // Todo: claiming the entry with a single CAS is difficult, since it is difficult to set all bits correctly
        // -> did the prior entry have a delimiter, what was the other meta data (if we have more meta data in the future)
        const auto currentEntry = this->getState();
        const auto currentABAItNo = currentEntry.getABAItNo();
        const auto currentHasCompletedLeading = currentEntry.hasUsedLeadingBuffer();
        const auto currentHasCompletedTrailing = currentEntry.hasUsedTrailingBuffer();
        const auto priorEntryIsUsed = currentABAItNo == (abaItNumber - 1) and currentHasCompletedLeading and currentHasCompletedTrailing;
        if (priorEntryIsUsed)
        {
            this->leadingBufferRefCount = 1;
            this->trailingBufferRefCount = 1;
            if (HasTupleDelimiter)
            {
                this->atomicBitmapState.setHasTupleDelimiterState(abaItNumber);
            }
            else
            {
                this->atomicBitmapState.setNoTupleDelimiterState(abaItNumber);
            }
        }
        return priorEntryIsUsed;
    }

    bool tryClaimSpanningTuple(const uint32_t abaItNumber)
    {
        if (this->atomicBitmapState.tryClaimSpanningTuple(abaItNumber))
        {
            --this->trailingBufferRefCount;
            this->atomicBitmapState.setUsedTrailingBuffer();
            return true;
        }
        return false;
    }
    void setStateOfFirstIndex()
    {
        /// The first entry is a dummy that makes sure that we can resolve the first tuple in the first buffer
        this->leadingBufferRefCount = 0;
        this->trailingBufferRefCount = 1;
        this->atomicBitmapState.setStateOfFirstEntry();
    }

    void claimNoDelimiterBuffer()
    {
        /// First claim buffer uses
        --this->leadingBufferRefCount;
        --this->trailingBufferRefCount;
        INVARIANT(this->leadingBufferRefCount == 0, "Leading count expected to be 0 but was", this->leadingBufferRefCount);
        INVARIANT(this->trailingBufferRefCount == 0, "Trailing count expected to be 0, but was", this->trailingBufferRefCount);

        /// Then atomically mark buffer as used
        this->atomicBitmapState.setUsedLeadingAndTrailingBuffer();
    }

    void claimLeadingBuffer()
    {
        --this->leadingBufferRefCount;
        INVARIANT(this->leadingBufferRefCount == 0, "Leading count expected to be 0, but was", this->leadingBufferRefCount);
        this->atomicBitmapState.setUsedLeadingBuffer();
    }

    [[nodiscard]] AtomicBitmapState::BitmapState getState() const { return this->atomicBitmapState.getState(); }
    [[nodiscard]] int8_t getLeadingBufferRefCount() const { return this->leadingBufferRefCount; }
    [[nodiscard]] int8_t getTrailingBufferRefCount() const { return this->trailingBufferRefCount; }
    [[nodiscard, maybe_unused]] uint32_t getFirstDelimiterOffset() const { return this->firstDelimiterOffset; }
    [[nodiscard, maybe_unused]] uint32_t getLastDelimiterOffset() const { return this->lastDelimiterOffset; }

private:
    // 24 Bytes (TupleBuffer)
    int8_t leadingBufferRefCount;
    // 48 Bytes (TupleBuffer)
    int8_t trailingBufferRefCount;
    // 56 Bytes (Atomic state)
    AtomicBitmapState atomicBitmapState;
    // 60 Bytes (meta data)
    uint32_t firstDelimiterOffset{};
    // 64 Bytes (meta data)
    uint32_t lastDelimiterOffset{};
};

FMT_OSTREAM(::AtomicBitmapState);
FMT_OSTREAM(::Interval);

uint32_t getABAItNumber(const size_t sequenceNumber, const size_t rbSize)
{
    return static_cast<uint32_t>(sequenceNumber / rbSize) + 1;
}

template <bool IsLeading>
bool hasNoTD(const size_t snRBIdx, const size_t abaItNumber, const size_t distance, const std::vector<SSMetaData>& tds)
{
    const auto adjustedSN = (IsLeading) ? snRBIdx - distance : snRBIdx + distance;
    const auto tdsIdx = adjustedSN % tds.size();
    const auto bitmapState = tds[tdsIdx].getState();
    if (IsLeading)
    {
        const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < distance);
        return bitmapState.hasNoTupleDelimiter() == 1 and bitmapState.getABAItNo() == adjustedAbaItNumber;
    }
    const auto adjustedAbaItNumber = abaItNumber + static_cast<size_t>((snRBIdx + distance) >= tds.size());
    return bitmapState.hasNoTupleDelimiter() == 1 and bitmapState.getABAItNo() == adjustedAbaItNumber;
}

template <bool IsLeading>
bool hasTD(const size_t snRBIdx, const size_t abaItNumber, const size_t distance, const std::vector<SSMetaData>& tds)
{
    const auto adjustedSN = (IsLeading) ? snRBIdx - distance : snRBIdx + distance;
    const auto tdsIdx = adjustedSN % tds.size();
    const auto bitmapState = tds[tdsIdx].getState();
    if (IsLeading)
    {
        const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < distance);
        return bitmapState.hasTupleDelimiter() == 1 and bitmapState.getABAItNo() == adjustedAbaItNumber;
    }
    const auto adjustedAbaItNumber = abaItNumber + static_cast<size_t>((snRBIdx + distance) >= tds.size());
    return bitmapState.hasTupleDelimiter() == 1 and bitmapState.getABAItNo() == adjustedAbaItNumber;
}

void setCompletedFlags(const size_t firstDelimiter, const size_t lastDelimiter, std::vector<SSMetaData>& ringBuffer)
{
    size_t nextDelimiter = (firstDelimiter + 1) % ringBuffer.size();
    while (nextDelimiter != lastDelimiter)
    {
        ringBuffer[nextDelimiter].claimNoDelimiterBuffer();
        nextDelimiter = (nextDelimiter + 1) % ringBuffer.size();
    }
    ringBuffer[lastDelimiter].claimLeadingBuffer();
}

class TimedLatch
{
private:
    std::latch latch_;
    std::condition_variable cv_;
    std::mutex mutex_;

public:
    TimedLatch(std::ptrdiff_t count) : latch_(count) { }

    void count_down()
    {
        latch_.count_down();
        cv_.notify_all();
    }

    void wait() { latch_.wait(); }

    template <typename Rep, typename Period>
    bool wait_for(const std::chrono::duration<Rep, Period>& timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this]() { return latch_.try_wait(); });
    }
};

void executeRingBufferExperiment(
    const size_t NUM_THREADS, const size_t NUM_SEQUENCE_NUMBERS, const size_t SIZE_OF_RING_BUFFER, const double tdLikelihood)
{
    // assert(NUM_SEQUENCE_NUMBERS % NUM_THREADS == 0);
    assert(SIZE_OF_RING_BUFFER > NUM_THREADS);
    // Todo: assert than max allowed index of ring buffer is evenly divisible by SIZE_OF_RING_BUFFER (allows overflow wrapping)
    std::vector<SSMetaData> ringBuffer(SIZE_OF_RING_BUFFER);
    ringBuffer[0].setStateOfFirstIndex();

    std::vector<std::jthread> threads{};
    threads.reserve(NUM_THREADS);

    std::vector<std::vector<Interval>> threadLocalResultIntervals(NUM_THREADS);

    TimedLatch completionLatch(NUM_THREADS);

    for (size_t i = 0; i < NUM_THREADS; ++i)
    {
        threads.emplace_back(
            [i, &ringBuffer, NUM_THREADS, NUM_SEQUENCE_NUMBERS, &threadLocalResultIntervals, &completionLatch, tdLikelihood]()
            {
                std::mt19937_64 sequenceNumberGen(i);
                std::bernoulli_distribution boolDistribution{tdLikelihood};

                size_t currentSequenceNumber = i + 1;

                const auto leadingDelimiterSearch
                    = [&ringBuffer](const size_t snRBIdx, const size_t abaItNumber, const size_t currentSequenceNumber) -> uint32_t
                {
                    size_t leadingDistance = 1;
                    while (hasNoTD<true>(snRBIdx, abaItNumber, leadingDistance, ringBuffer))
                    {
                        ++leadingDistance;
                    }
                    return hasTD<true>(snRBIdx, abaItNumber, leadingDistance, ringBuffer) ? (currentSequenceNumber - leadingDistance)
                                                                                          : std::numeric_limits<uint32_t>::max();
                };

                const auto trailingDelimiterSearch
                    = [&ringBuffer](const size_t snRBIdx, const size_t abaItNumber, const size_t currentSequenceNumber) -> uint32_t
                {
                    size_t trailingDistance = 1;
                    while (hasNoTD<false>(snRBIdx, abaItNumber, trailingDistance, ringBuffer))
                    {
                        ++trailingDistance;
                    }
                    return hasTD<false>(snRBIdx, abaItNumber, trailingDistance, ringBuffer) ? (currentSequenceNumber + trailingDistance)
                                                                                            : std::numeric_limits<uint32_t>::max();
                };

                const auto getTupleDelimiter
                    = [&boolDistribution, &sequenceNumberGen, &NUM_THREADS, NUM_SEQUENCE_NUMBERS](
                          const size_t noTupleDelimiterSeqCount, const size_t ringBufferSize, const size_t currentSequenceNumber)
                {
                    if (currentSequenceNumber == NUM_SEQUENCE_NUMBERS)
                    {
                        return true;
                    }
                    const bool isTupleDelimiter = boolDistribution(sequenceNumberGen);
                    const auto maxIterationsWithoutDelimiter = (ringBufferSize - NUM_THREADS) / NUM_THREADS;
                    /// Make sure there never is a sequence of 'no tuple delimiters' that is larger than the size of the ringbuffer - 1
                    /// As long as the size of the ring buffer is larger than the number of threads, this avoids a deadlock, where the
                    /// ring buffer has no, or only a single tuple delimiter and all sequence numbers are stuck in the prior iteration.
                    if (not isTupleDelimiter and noTupleDelimiterSeqCount + 1 >= maxIterationsWithoutDelimiter)
                    {
                        return true;
                    }
                    return isTupleDelimiter;
                };

                size_t noTupleDelimiterSeqCount = 0;
                while (currentSequenceNumber <= NUM_SEQUENCE_NUMBERS)
                {
                    const auto abaItNumber = getABAItNumber(currentSequenceNumber, ringBuffer.size());
                    const auto snRBIdx = currentSequenceNumber % ringBuffer.size();

                    const bool hasTupleDelimiter = getTupleDelimiter(noTupleDelimiterSeqCount, ringBuffer.size(), currentSequenceNumber);
                    if (hasTupleDelimiter)
                    {
                        if (not ringBuffer[snRBIdx].isInRange<true>(abaItNumber))
                        {
                            continue;
                        }
                        noTupleDelimiterSeqCount = 0;

                        const auto firstDelimiter = leadingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);
                        const auto lastDelimiter = trailingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);
                        if (firstDelimiter != std::numeric_limits<uint32_t>::max())
                        {
                            const auto firstDelimiterIdx = firstDelimiter % ringBuffer.size();
                            const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > snRBIdx);

                            if (ringBuffer[firstDelimiterIdx].tryClaimSpanningTuple(abaItNumberOfFirstDelimiter))
                            {
                                /// Successfully claimed the first tuple, now set the rest
                                setCompletedFlags(firstDelimiterIdx, snRBIdx, ringBuffer);
                                threadLocalResultIntervals.at(i).emplace_back()
                                    = Interval{.lo = firstDelimiter, .hi = static_cast<uint32_t>(currentSequenceNumber)};
                            }
                        }
                        if (lastDelimiter != std::numeric_limits<uint32_t>::max())
                        {
                            if (ringBuffer[snRBIdx].tryClaimSpanningTuple(abaItNumber))
                            {
                                const auto lastDelimiterIdx = lastDelimiter % ringBuffer.size();
                                setCompletedFlags(snRBIdx, lastDelimiterIdx, ringBuffer);
                                threadLocalResultIntervals.at(i).emplace_back()
                                    = Interval{.lo = static_cast<uint32_t>(currentSequenceNumber), .hi = lastDelimiter};
                            }
                        }
                    }
                    else
                    {
                        if (not ringBuffer[snRBIdx].isInRange<true>(abaItNumber))
                        {
                            continue;
                        }
                        ++noTupleDelimiterSeqCount;
                        const auto firstDelimiter = leadingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);
                        const auto lastDelimiter = trailingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);

                        if (firstDelimiter != std::numeric_limits<uint32_t>::max()
                            and lastDelimiter != std::numeric_limits<uint32_t>::max())
                        {
                            const auto firstDelimiterIdx = firstDelimiter % ringBuffer.size();
                            const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > snRBIdx);
                            if (ringBuffer[firstDelimiterIdx].tryClaimSpanningTuple(abaItNumberOfFirstDelimiter))
                            {
                                /// Successfully claimed the first tuple, now set the rest
                                setCompletedFlags(firstDelimiterIdx, lastDelimiter % ringBuffer.size(), ringBuffer);
                                threadLocalResultIntervals.at(i).emplace_back() = Interval{.lo = firstDelimiter, .hi = lastDelimiter};
                            }
                        }
                    }
                    currentSequenceNumber += NUM_THREADS;
                }
                completionLatch.count_down();
            });
    }
    if (not completionLatch.wait_for(std::chrono::seconds(20)))
    {
        std::exit(1);
    }

    /// Validate that thread local intervals overall add up to exactly the number of sequence numbers processed
    /// Example for 4 sequence numbers: [0-1], [1-3], [3-4] --> (1-0) + (3-1) + (4-3) = 1 + 2 + 1 = 4
    size_t globalCheckSum = std::accumulate(
        threadLocalResultIntervals.begin(),
        threadLocalResultIntervals.end(),
        0,
        [](const size_t runningSum, const std::vector<Interval>& threadLocalIntervals)
        {
            return std::accumulate(
                threadLocalIntervals.begin(),
                threadLocalIntervals.end(),
                runningSum,
                [](const size_t globalRunningSum, const Interval& interval) { return globalRunningSum + (interval.hi - interval.lo); });
        });
    INVARIANT(
        globalCheckSum == NUM_SEQUENCE_NUMBERS, "Expected sum of intervals to be: {}, but was: {}", globalCheckSum, NUM_SEQUENCE_NUMBERS);

    /// Check that the usages for all buffers are exactly 0
    for (const auto& [idx, metaData] : ringBuffer | NES::views::enumerate)
    {
        if (std::cmp_greater(idx, NUM_SEQUENCE_NUMBERS))
        {
            break;
        }
        INVARIANT(
            metaData.getLeadingBufferRefCount() == 0,
            "Buffer at index {} had leading buffer count other than 0: {}",
            idx,
            metaData.getLeadingBufferRefCount());
        /// The very last element of the ring buffer will still have a trailing buffer use
        if (static_cast<size_t>(idx) != (NUM_SEQUENCE_NUMBERS % ringBuffer.size()))
        {
            INVARIANT(
                metaData.getTrailingBufferRefCount() == 0,
                "Buffer at index {} had trailing buffer count other than 0: {}",
                idx,
                metaData.getTrailingBufferRefCount());
        }
    }
}

int main()
{
    // std::cout << sizeof(AtomicBitmapState) << std::endl;
    // executeRingBufferExperiment(1, 1000, 64, 0.5);
    executeRingBufferExperiment(23, 10000000, 64, 0.5);
    return 0;
}
