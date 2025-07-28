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
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <latch>
#include <limits>
#include <memory>
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

#include <DataTypes/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <condition_variable>


#include <Util/Ranges.hpp>

// 33: 000000000000000000000000000000100000000000000000000000000000000 <- 4294967296
// 34: 000000000000000000000000000001000000000000000000000000000000000 <- 8589934592
// 35: 000000000000000000000000000010000000000000000000000000000000000 <-- 17179869184
// 36: 000000000000000000000000000100000000000000000000000000000000000 <-- 34359738368

class AtomicBitmapState
{
    static constexpr uint64_t hasTupleDelimiterBit = 4294967296ULL;
    static constexpr uint64_t noTupleDelimiterBit = 8589934592ULL;
    static constexpr uint64_t completedLeadingBit = 17179869184ULL;
    static constexpr uint64_t completedTrailingBit = 34359738368ULL;
public:
    class BitmapState
    {
    public:
        explicit BitmapState(const uint64_t state) : state(state) {};
        [[nodiscard]] uint32_t getABAItNo() const { return static_cast<uint32_t>(state); }
        [[nodiscard]] bool hasTupleDelimiter() const { return (state & hasTupleDelimiterBit) == hasTupleDelimiterBit; }
        [[nodiscard]] bool hasNoTupleDelimiter() const { return (state & noTupleDelimiterBit) == noTupleDelimiterBit; }
        [[nodiscard]] bool hasCompletedLeading() const { return (state & completedLeadingBit) == completedLeadingBit; }
        [[nodiscard]] bool hasCompletedTrailing() const { return (state & completedTrailingBit) == completedTrailingBit; }

        // Todo: make private and hide behind helper functions?
        void setCompletedLeadingAndTrailing()
        {
            constexpr uint64_t completedLeadingAndTrailingBits = 51539607552ULL;
            this->state |= completedLeadingAndTrailingBits;
        }

        void updateLeading(const BitmapState other)
        {
            this->state |= (other.getBitmapState() & completedLeadingBit);
        }

        void setCompletedTrailing()
        {
            this->state |= completedTrailingBit;
        }

    private:
        friend class AtomicBitmapState;
        [[nodiscard]] uint64_t getBitmapState() const { return state; }
        uint64_t state;
    };
    // Todo: rethink separating state into non atomic class and wrapping
    // - advantage: can operate on state without atomic operations
    // - disadvantage: how to implement e.g., 'setCompletedLeading' via or, without two function calls (one nested)
    AtomicBitmapState() : state(51539607552) {};
    // explicit AtomicBitmapState(const uint64_t initialState) : state(initialState) {};
    ~AtomicBitmapState() = default;

    AtomicBitmapState(const AtomicBitmapState&) = delete;
    AtomicBitmapState(const AtomicBitmapState&&) = delete;
    void operator=(const AtomicBitmapState& other) = delete;
    void operator=(const AtomicBitmapState&& other) = delete;

    BitmapState getState() const { return BitmapState{this->state.load()}; }

    bool tryClaimSpanningTuple(const uint32_t abaItNumber)
    {
        auto atomicFirstDelimiter = getState();

        auto desiredFirstDelimiter = atomicFirstDelimiter;
        desiredFirstDelimiter.setCompletedTrailing();

        bool claimedSpanningTuple = false;
        while (atomicFirstDelimiter.getABAItNo() == abaItNumber and not atomicFirstDelimiter.hasCompletedTrailing()
               and not claimedSpanningTuple)
        {
            desiredFirstDelimiter.updateLeading(atomicFirstDelimiter);
            claimedSpanningTuple = this->state.compare_exchange_weak(atomicFirstDelimiter.state, desiredFirstDelimiter.state);
        }
        return claimedSpanningTuple;
    }

    void setNewState(const BitmapState& newState)
    {
        this->state = newState.getBitmapState();
    }

    [[nodiscard]] uint32_t getABAItNo() const
    {
        /// The first 32 bits are the ABA number
        return static_cast<uint32_t>(state.load());
    }

    [[nodiscard]] bool getHasTupleDelimiter() const
    {
        const auto currentState = state.load();
        const bool hasTupleDelimiter = (currentState & hasTupleDelimiterBit) == hasTupleDelimiterBit;
        return hasTupleDelimiter;
    }

    [[nodiscard]] bool getNoTupleDelimiter() const
    {
        const auto currentState = state.load();
        const bool noTupleDelimiter = (currentState & noTupleDelimiterBit) == noTupleDelimiterBit;
        return noTupleDelimiter;
    }

    [[nodiscard]] bool getCompletedLeading() const
    {
        const auto currentState = state.load();
        const bool completedLeading = (currentState & completedLeadingBit) == completedLeadingBit;
        return completedLeading;
    }

    [[nodiscard]] bool getCompletedTrailing() const
    {
        const auto currentState = state.load();
        const bool completedTrailing = (currentState & completedTrailingBit) == completedTrailingBit;
        return completedTrailing;
    }

    void setCompletedLeading()
    {
        this->state |= completedLeadingBit;
    }

    void setCompletedTrailing()
    {
        this->state |= completedTrailingBit;
    }

    friend std::ostream& operator<<(std::ostream& os, const AtomicBitmapState& atomicBitmapState)
     {
         return os << fmt::format(
                    "[{}-{}-{}-{}-{}]",
                    atomicBitmapState.getHasTupleDelimiter(),
                    atomicBitmapState.getNoTupleDelimiter(),
                    atomicBitmapState.getABAItNo(),
                    atomicBitmapState.getCompletedLeading(),
                    atomicBitmapState.getCompletedTrailing());
     }
private:
    /// [1-32] : Tag (against ABA)
    /// [33]   : HasTupleDelimiter
    /// [34]   : NoTupleDelimiter
    /// [35]   : CompletedLeading
    /// [36]   : CompletedTrailing
    std::atomic<uint64_t> state;
};


// struct TupleDelimiterFastLaneRB
// {
//     uint8_t hasTupleDelimiter = false;
//     uint8_t noTupleDelimiter = false;
//     bool completedLeading = true;
//     bool completedTrailing = true;
//     uint32_t abaItNumber = 0;
//
//     friend std::ostream& operator<<(std::ostream& os, const TupleDelimiterFastLaneRB& interval)
//     {
//         return os << fmt::format(
//                    "[{}-{}-{}-{}-{}]",
//                    static_cast<int32_t>(interval.hasTupleDelimiter),
//                    static_cast<int32_t>(interval.noTupleDelimiter),
//                    interval.abaItNumber,
//                    interval.completedLeading,
//                    interval.completedTrailing);
//     }
//     bool operator==(const TupleDelimiterFastLaneRB& desired_first_delimiter) const = default;
// };
//
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
FMT_OSTREAM(::AtomicBitmapState);
FMT_OSTREAM(::Interval);

uint32_t getABAItNumber(const size_t sequenceNumber, const size_t rbSize)
{
    return static_cast<uint32_t>(sequenceNumber / rbSize) + 1;
}

template <bool IsLeading>
bool hasNoTD(
    const size_t snRBIdx, const size_t abaItNumber, const size_t distance, const std::vector<AtomicBitmapState>& tds)
{
    const auto adjustedSN = (IsLeading) ? snRBIdx - distance : snRBIdx + distance;
    const auto tdsIdx = adjustedSN % tds.size();
    const auto bitmapState = tds[tdsIdx].getState();
    // Todo: is grossly wrong <-- need to adjust 'IsLeading' aware and also when wrapping to MAX_VALUE
    // -> only adjust if wrapping around
    if (IsLeading)
    {
        const auto adjustedAbaItNumber = abaItNumber - static_cast<size_t>(snRBIdx < distance);
        return bitmapState.hasNoTupleDelimiter() == 1 and bitmapState.getABAItNo() == adjustedAbaItNumber;
    }
    const auto adjustedAbaItNumber = abaItNumber + static_cast<size_t>((snRBIdx + distance) >= tds.size());
    return bitmapState.hasNoTupleDelimiter() == 1 and bitmapState.getABAItNo() == adjustedAbaItNumber;
}

template <bool IsLeading>
bool hasTD(
    const size_t snRBIdx, const size_t abaItNumber, const size_t distance, const std::vector<AtomicBitmapState>& tds)
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

void setCompletedFlags(
    const size_t firstDelimiter, const size_t lastDelimiter, std::vector<AtomicBitmapState>& ringBuffer)
{
    size_t nextDelimiter = (firstDelimiter + 1) % ringBuffer.size();
    while (nextDelimiter != lastDelimiter)
    {
        auto newMiddleEntry = ringBuffer[nextDelimiter].getState();
        newMiddleEntry.setCompletedLeadingAndTrailing();
        ringBuffer[nextDelimiter].setNewState(newMiddleEntry);
        nextDelimiter = (nextDelimiter + 1) % ringBuffer.size();
    }

    // TOdo: problem
    // -> between loading newLastEntry and setting it again below, another thread may have changed newLastEntry already
    // -> since it is 'completing', no other thread should advance entry to next ABA iteration
    // -> this should be the only thread that can set entry
    // -> however, another thread may set 'completedTrailing' <-- todo: make some of the values const of atomic struct?
    // -> Could: use bitmap and 'or' with bitmap <-- todo: would require redesign
    // Current solution:
    // -> CAS loop, make sure value is expected and update if not, retry until successful
    ringBuffer[lastDelimiter].setCompletedLeading();
    // auto lastEntry = ringBuffer[lastDelimiter].load();
    // auto newLastEntry = lastEntry;
    // // ringBuffer[lastDelimiter] = newLastEntry;
    // newLastEntry.completedLeading = true;
    // while (not ringBuffer[lastDelimiter].compare_exchange_weak(lastEntry, newLastEntry))
    // {
    //     newLastEntry.completedTrailing = lastEntry.completedTrailing;
    // }
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
    std::vector<AtomicBitmapState> ringBuffer(SIZE_OF_RING_BUFFER);
    ringBuffer[0].setNewState(AtomicBitmapState::BitmapState{21474836481ULL});

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

                const auto getTupleDelimiter = [&boolDistribution, &sequenceNumberGen, &NUM_THREADS](
                                                   const size_t noTupleDelimiterSeqCount, const size_t ringBufferSize)
                {
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

                // const auto claimSpanningTuple = [&ringBuffer](const size_t firstDelimiter, const uint32_t abaItNumber) -> bool
                // {
                //     const auto firstDelimiterIdx = firstDelimiter % ringBuffer.size();
                //     auto atomicFirstDelimiter = ringBuffer[firstDelimiterIdx].getState();
                //
                //     auto desiredFirstDelimiter = atomicFirstDelimiter;
                //     desiredFirstDelimiter.completedTrailing = true;
                //     bool claimedSpanningTuple = false;
                //     while (atomicFirstDelimiter.abaItNumber == abaItNumber and not atomicFirstDelimiter.completedTrailing
                //            and not claimedSpanningTuple)
                //     {
                //         desiredFirstDelimiter.completedLeading = atomicFirstDelimiter.completedLeading;
                //         claimedSpanningTuple
                //             = ringBuffer[firstDelimiterIdx].compare_exchange_weak(atomicFirstDelimiter, desiredFirstDelimiter);
                //     }
                //     return claimedSpanningTuple;
                // };

                size_t noTupleDelimiterSeqCount = 0;
                // size_t retryCount = 0;
                while (currentSequenceNumber < NUM_SEQUENCE_NUMBERS)
                {
                    const auto abaItNumber = getABAItNumber(currentSequenceNumber, ringBuffer.size());
                    const auto snRBIdx = currentSequenceNumber % ringBuffer.size();
                    const auto currentEntry = ringBuffer[snRBIdx].getState();
                    const auto currentABAItNo = currentEntry.getABAItNo();
                    const auto currentHasCompletedLeading = currentEntry.hasCompletedLeading();
                    const auto currentHasCompletedTrailing = currentEntry.hasCompletedTrailing();
                    if (currentABAItNo != (abaItNumber - 1) or not currentHasCompletedLeading or not currentHasCompletedTrailing)
                    {
                        continue;
                    }

                    const bool hasTupleDelimiter = getTupleDelimiter(noTupleDelimiterSeqCount, ringBuffer.size());
                    if (hasTupleDelimiter)
                    {
                        noTupleDelimiterSeqCount = 0;
                        ringBuffer[snRBIdx].setNewState(AtomicBitmapState::BitmapState{4294967296ULL | abaItNumber});
                        // ringBuffer[snRBIdx] = TupleDelimiterFastLaneRB{
                        //     .hasTupleDelimiter = 1,
                        //     .noTupleDelimiter = 0,
                        //     .completedLeading = false,
                        //     .completedTrailing = false,
                            // .abaItNumber = abaItNumber};

                        const auto firstDelimiter = leadingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);
                        const auto lastDelimiter = trailingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);
                        if (firstDelimiter != std::numeric_limits<uint32_t>::max())
                        {
                            // TODO: CONSTRUCTION SITE
                            /// The leading ST ends in the snRBIdx, if its index is larger than the snRBIdx, it is from a prior iteration
                            const auto firstDelimiterIdx = firstDelimiter % ringBuffer.size();
                            const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > snRBIdx);

                            // if (claimSpanningTuple(firstDelimiterIdx, abaItNumberOfFirstDelimiter))
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
                            // const auto lastDelimiterIdx = lastDelimiter % ringBuffer.size();
                            // const auto abaItNumberOfLastDelimiter = abaItNumber - static_cast<size_t>(lastDelimiterIdx < snRBIdx);
                            // if (claimSpanningTuple(snRBIdx, abaItNumber))
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
                        ++noTupleDelimiterSeqCount;
                        ringBuffer[snRBIdx].setNewState(AtomicBitmapState::BitmapState{8589934592ULL | abaItNumber});
                        // ringBuffer[snRBIdx] = TupleDelimiterFastLaneRB{
                        //     .hasTupleDelimiter = 0,
                        //     .noTupleDelimiter = 1,
                        //     .completedLeading = false,
                        //     .completedTrailing = false,
                        //     .abaItNumber = abaItNumber};
                        const auto firstDelimiter = leadingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);
                        const auto lastDelimiter = trailingDelimiterSearch(snRBIdx, abaItNumber, currentSequenceNumber);

                        if (firstDelimiter != std::numeric_limits<uint32_t>::max()
                            and lastDelimiter != std::numeric_limits<uint32_t>::max())
                        {
                            const auto firstDelimiterIdx = firstDelimiter % ringBuffer.size();
                            const auto abaItNumberOfFirstDelimiter = abaItNumber - static_cast<size_t>(firstDelimiterIdx > snRBIdx);
                            // if (claimSpanningTuple(firstDelimiterIdx, abaItNumberOfFirstDelimiter))
                            if (ringBuffer[firstDelimiterIdx].tryClaimSpanningTuple(abaItNumberOfFirstDelimiter))
                            {
                                /// Successfully claimed the first tuple, now set the rest
                                setCompletedFlags(firstDelimiterIdx, lastDelimiter % ringBuffer.size(), ringBuffer);
                                threadLocalResultIntervals.at(i).emplace_back() = Interval{.lo = firstDelimiter, .hi = lastDelimiter};
                            }
                            // setCompletedFlags(firstDelimiter % ringBuffer.size(), lastDelimiter % ringBuffer.size(), ringBuffer);
                            // threadLocalResultIntervals.at(i).emplace_back() = Interval{.lo = firstDelimiter, .hi = lastDelimiter};
                        }
                    }
                    currentSequenceNumber += NUM_THREADS;
                }
                completionLatch.count_down();
            });
    }
    // completionLatch.wait();
    const auto success = completionLatch.wait_for(std::chrono::seconds(5));
    if (not success)
    {
        auto combinedThreadResults = std::ranges::views::join(threadLocalResultIntervals);
        std::vector<Interval> resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());
        std::ranges::sort(
            resultBufferVec.begin(),
            resultBufferVec.end(),
            [](const Interval& leftInterval, const Interval& rightInterval) { return leftInterval < rightInterval; });
        for (const auto& [idx, ringBuffer] : ringBuffer | NES::views::enumerate)
        {
            fmt::println("{}: {}", idx, ringBuffer);
        }
        fmt::println("{}", fmt::join(ringBuffer, ","));
        fmt::println("Sorted results: {}", fmt::join(resultBufferVec, ","));
        for (size_t i = 0; i < resultBufferVec.size() - 1; ++i)
        {
            if (resultBufferVec[i].hi != resultBufferVec[i + 1].lo)
            {
                if (resultBufferVec[i].lo == resultBufferVec[i + 1].lo)
                {
                    fmt::println("{} x duplicate connection with {}", resultBufferVec[i], resultBufferVec[i + 1]);
                }
                else
                {
                    fmt::println("{} x missing connection with {}", resultBufferVec[i], resultBufferVec[i + 1]);
                }
            }
            else
            {
                fmt::println("{} <-- connects: {}-{}", resultBufferVec[i], resultBufferVec[i], resultBufferVec[i + 1]);
            }
        }
        std::exit(1);
    }
    // auto combinedThreadResults = std::ranges::views::join(threadLocalResultIntervals);
    // std::vector<Interval> resultBufferVec(combinedThreadResults.begin(), combinedThreadResults.end());
    // std::ranges::sort(
    //     resultBufferVec.begin(),
    //     resultBufferVec.end(),
    //     [](const Interval& leftInterval, const Interval& rightInterval) { return leftInterval < rightInterval; });
    // for (const auto& [idx, ringBuffer] : ringBuffer | NES::views::enumerate)
    // {
    //     fmt::println("{}: {}", idx, ringBuffer);
    // }
    // fmt::println("{}", fmt::join(ringBuffer, ","));
    // fmt::println("Sorted results: {}", fmt::join(resultBufferVec, ","));
    // for (size_t i = 0; i < resultBufferVec.size() - 1; ++i)
    // {
    //     if (resultBufferVec[i].hi != resultBufferVec[i + 1].lo)
    //     {
    //         if (resultBufferVec[i].lo == resultBufferVec[i + 1].lo)
    //         {
    //             fmt::println("{} x duplicate connection with {}", resultBufferVec[i], resultBufferVec[i + 1]);
    //         }
    //         else
    //         {
    //             fmt::println("{} x missing connection with {}", resultBufferVec[i], resultBufferVec[i + 1]);
    //         }
    //     }
    //     else
    //     {
    //         fmt::println("{} <-- connects: {}-{}", resultBufferVec[i], resultBufferVec[i], resultBufferVec[i + 1]);
    //     }
    // }
}

void syncLessIncreaseExperiment(const size_t NUM_THREADS, const size_t upperBound)
{
    std::vector<std::jthread> threads{};

    size_t counter = 0;
    std::latch completionLatch(NUM_THREADS);
    std::vector<size_t> threadLocalIncrementCounter(NUM_THREADS);

    for (size_t i = 0; i < NUM_THREADS; ++i)
    {
        threads.emplace_back(
            [i, &counter, &completionLatch, &upperBound, &threadLocalIncrementCounter, NUM_THREADS]()
            {
                size_t nextThreadCounter = i;
                while (counter < upperBound)
                {
                    if (counter == nextThreadCounter)
                    {
                        counter = std::min(counter + 1, upperBound);
                        ++threadLocalIncrementCounter[i];
                        nextThreadCounter += NUM_THREADS;
                    }
                }
                completionLatch.count_down();
            });
    }
    completionLatch.wait();
    fmt::println("Upperbound: {}, final counter: {}", upperBound, counter);
    fmt::println("Thread local counters: {}", fmt::join(threadLocalIncrementCounter, ","));
}

int main()
{
    // std::cout << sizeof(AtomicBitmapState) << std::endl;
    executeRingBufferExperiment(23, 10000000, 64, 0.5);
    // executeRingBufferExperiment(23, 10000000, 1024, 0.1);
    return 0;
}
