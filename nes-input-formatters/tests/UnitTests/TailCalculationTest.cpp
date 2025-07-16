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
#include <cassert>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <gtest/gtest.h>
#include <yaml-cpp/node/parse.h>
#include "Runtime/NodeEngine.hpp"

// #include <BaseUnitTest.hpp>
#include <optional>
#include <ErrorHandling.hpp>
#include <InputFormatterTestUtil.hpp>
#include <TestTaskQueue.hpp>


double executeLockLess(const size_t NUMBER_OF_TASKS, const size_t SIZE_OF_BUFFERS, const size_t NUMBER_OF_THREADS)
{
    using namespace NES;

    std::vector<std::atomic<uint16_t>> markAsSeen(NUMBER_OF_TASKS);
    std::vector<std::atomic<uint16_t>> markAsConnected(NUMBER_OF_TASKS);
    markAsSeen[0] = 1;
    const auto executeFunction = [&markAsSeen, &markAsConnected](const Memory::TupleBuffer& tupleBuffer, PipelineExecutionContext&)
    {
        const auto sequenceNumber = tupleBuffer.getSequenceNumber().getRawValue();
        markAsSeen[sequenceNumber] = 1;

        if (markAsSeen[sequenceNumber - 1] != 0)
        {
            markAsConnected[sequenceNumber - 1] = 1;
        }
        if (markAsSeen[sequenceNumber + 1] != 0)
        {
            markAsConnected[sequenceNumber + 1] = 1;
        }
    };

    /// Create test task queue and process input formatter tasks
    std::vector<double> finalProcessingTimes;
    {
        std::shared_ptr<Memory::BufferManager> testBufferPool = Memory::BufferManager::create(SIZE_OF_BUFFERS, NUMBER_OF_TASKS);
        std::vector<TestPipelineTask> pipelineTasks;
        for (size_t taskIdx = 0; taskIdx < NUMBER_OF_TASKS; ++taskIdx)
        {
            auto taskBuffer = testBufferPool->getBufferBlocking();
            taskBuffer.setSequenceNumber(SequenceNumber(taskIdx + 1));
            auto testPipelineStage = std::make_shared<TestPipelineStage>(std::to_string(taskIdx), executeFunction);
            pipelineTasks.emplace_back(TestPipelineTask(INVALID<WorkerThreadId>, taskBuffer, testPipelineStage));
        }

        const auto taskQueue = std::make_unique<MultiThreadedTestTaskQueue>(
            NUMBER_OF_THREADS,
            std::move(pipelineTasks),
            std::move(testBufferPool),
            std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUMBER_OF_THREADS));

        fmt::println("Starting to process tasks");
        taskQueue->startProcessing();
        const auto finalProcessingTime = taskQueue->waitForCompletion();
        finalProcessingTimes.emplace_back(finalProcessingTime);

        /// Validate
        const size_t markedAsSeenCount = std::accumulate(
            markAsSeen.begin(),
            markAsSeen.end(),
            static_cast<size_t>(0),
            [](size_t sum, const auto& atomicVal) { return sum + atomicVal; });
        if (markedAsSeenCount != NUMBER_OF_TASKS)
        {
            std::cerr << fmt::format("Expected {} tasks, but got {}.", NUMBER_OF_TASKS, markedAsSeenCount);
            std::exit(1);
        }
    }
    return finalProcessingTimes.front();
}

double executeLocked(const size_t NUMBER_OF_TASKS, const size_t SIZE_OF_BUFFERS, const size_t NUMBER_OF_THREADS)
{
    using namespace NES;

    std::mutex readWriteMutex;
    std::vector<std::atomic<uint16_t>> markAsSeen(NUMBER_OF_TASKS);
    std::vector<std::atomic<uint16_t>> markAsConnected(NUMBER_OF_TASKS);
    markAsSeen[0] = 1;

    const auto executeFunction
        = [&readWriteMutex, &markAsSeen, &markAsConnected](const Memory::TupleBuffer& tupleBuffer, PipelineExecutionContext&)
    {
        std::scoped_lock counterLock(readWriteMutex);
        const auto sequenceNumber = tupleBuffer.getSequenceNumber().getRawValue();
        markAsSeen[sequenceNumber] = 1;
        if (markAsSeen[sequenceNumber - 1] != 0)
        {
            markAsConnected[sequenceNumber - 1] = 1;
        }
        if (markAsSeen[sequenceNumber + 1] != 0)
        {
            markAsConnected[sequenceNumber + 1] = 1;
        }
    };

    /// Create test task queue and process input formatter tasks
    std::vector<double> finalProcessingTimes;
    {
        std::shared_ptr<Memory::BufferManager> testBufferPool = Memory::BufferManager::create(SIZE_OF_BUFFERS, NUMBER_OF_TASKS);
        std::vector<TestPipelineTask> pipelineTasks;
        for (size_t taskIdx = 0; taskIdx < NUMBER_OF_TASKS; ++taskIdx)
        {
            auto taskBuffer = testBufferPool->getBufferBlocking();
            taskBuffer.setSequenceNumber(SequenceNumber(taskIdx + 1));
            auto testPipelineStage = std::make_shared<TestPipelineStage>(std::to_string(taskIdx), executeFunction);
            pipelineTasks.emplace_back(TestPipelineTask(INVALID<WorkerThreadId>, taskBuffer, testPipelineStage));
        }

        const auto taskQueue = std::make_unique<MultiThreadedTestTaskQueue>(
            NUMBER_OF_THREADS,
            std::move(pipelineTasks),
            std::move(testBufferPool),
            std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUMBER_OF_THREADS));

        fmt::println("Starting to process tasks");
        taskQueue->startProcessing();
        const auto finalProcessingTime = taskQueue->waitForCompletion();
        finalProcessingTimes.emplace_back(finalProcessingTime);

        /// Validate
        const auto markedAsSeenCount = std::accumulate(
            markAsSeen.begin(), markAsSeen.end(), static_cast<size_t>(0), [](size_t sum, const auto& val) { return sum + val; });
        if (markedAsSeenCount != NUMBER_OF_TASKS)
        {
            std::cerr << fmt::format("Expected {} tasks, but got {}.", NUMBER_OF_TASKS, markedAsSeenCount);
            std::exit(1);
        }
    }
    return finalProcessingTimes.front();
}

struct SampleStruct
{
    NES::Memory::TupleBuffer buffer;
    NES::Memory::TupleBuffer buffer2;
    std::atomic<uint16_t> sequenceNumber;
};

// Todo: try out lockless tail calculation mechanism
// - input intervals (generate random intervals and then shuffle them)
// - use task queue to let threads grap 'tailCalculation' tasks
// - validate by checking if tail is moved to final sequence number (hi of largest interval)

// Todo: generate intervals
struct Interval
{
    uint32_t lo;
    uint32_t hi;
};

class RandomNaturalGenerator
{
private:
    std::mt19937_64 rng;

public:
    explicit RandomNaturalGenerator(const std::uint64_t seed = std::random_device{}()) : rng(seed) { }

    // Generate a natural number > input with exponentially decreasing probability
    std::uint64_t generate(std::uint64_t input, double decay_rate)
    {
        // Use exponential distribution for the offset
        std::exponential_distribution<double> exp_dist(decay_rate);

        // Generate offset (distance from input)
        double offset = exp_dist(rng);

        // Convert to integer offset (at least 1 to ensure result > input)
        std::uint64_t int_offset = static_cast<std::uint64_t>(std::max(1.0, offset));
        // std::uint64_t int_offset = static_cast<std::uint64_t>(std::max(1.0, offset)) + 1;

        // Check for overflow
        if (input > std::numeric_limits<std::uint64_t>::max() - int_offset)
        {
            // If overflow would occur, use a smaller offset
            int_offset = std::numeric_limits<std::uint64_t>::max() - input;
        }

        return input + int_offset;
    }
    // Set new seed
    void set_seed(std::uint64_t seed) { rng.seed(seed); }
};

std::pair<std::vector<Interval>, Interval> generateIntervals(const size_t numIntervals, const uint64_t seed)
{
    RandomNaturalGenerator gen(seed);

    std::vector<Interval> intervals;

    size_t numGeneratedIntervals = 0;
    uint32_t currentHi = 1;
    while (numGeneratedIntervals < numIntervals)
    {
        const uint32_t newHi = gen.generate(currentHi, 0.3);
        intervals.emplace_back(Interval{currentHi, newHi});
        currentHi = newHi;
        ++numGeneratedIntervals;
    }
    const auto maxInterval = Interval{intervals.front().lo, intervals.back().hi};
    std::ranges::shuffle(intervals, std::mt19937(seed));
    return std::make_pair(intervals, maxInterval);
}


Interval calculateTail(const std::vector<Interval>& intervals, const uint32_t largestHi)
{
    std::vector<uint64_t> atomicIntervals(largestHi + 1);

    Interval largestMergeInterval{0, 0};
    for (const auto& interval : intervals)
    {
        largestMergeInterval = Interval{0, 0};
        // determine whether to chase to lowest
        const auto lo = atomicIntervals.at(interval.lo);
        /// If true, the current entry for low is obselete. Chase to lowest lo and then continue with hi, at the end set val of lowest lo
        if (lo != 0 and lo < interval.lo)
        {
            largestMergeInterval.lo = lo;
            auto lowestIndex = lo;
            if (atomicIntervals.at(largestMergeInterval.lo) < largestMergeInterval.lo)
            {
                lowestIndex = largestMergeInterval.lo;
                largestMergeInterval.lo = atomicIntervals.at(largestMergeInterval.lo);
            }
            // Don't set value for lowestIndex yet, since we haven't determined highest yet
            /// We found lowest lo, now chase to hi
            largestMergeInterval.hi = interval.hi;
            if (atomicIntervals.at(largestMergeInterval.hi) > largestMergeInterval.hi)
            {
                largestMergeInterval.hi = atomicIntervals.at(largestMergeInterval.hi);
            }
            atomicIntervals.at(lowestIndex) = largestMergeInterval.hi;
            atomicIntervals.at(largestMergeInterval.hi) = largestMergeInterval.lo;
            continue;
        }
        /// We know that we can't find a lower 'lo' than interval.low' (otherwise we would have taken first if condition)
        const auto hi = atomicIntervals.at(interval.hi);
        if (hi != 0 and hi > interval.hi)
        {
            largestMergeInterval.hi = hi;
            auto highestIndex = hi;
            if (atomicIntervals.at(largestMergeInterval.hi) > largestMergeInterval.hi)
            {
                highestIndex = largestMergeInterval.hi;
                largestMergeInterval.hi = atomicIntervals.at(largestMergeInterval.hi);
            }
            atomicIntervals[highestIndex] = interval.lo;
            atomicIntervals[interval.lo] = largestMergeInterval.hi;

        }
        else
        {
            atomicIntervals[interval.hi] = interval.lo;
            atomicIntervals[interval.lo] = interval.hi;
        }
    }
    return largestMergeInterval;
}
do:

Interval calculateTailBitmap(const std::vector<Interval>& intervals, const uint32_t largestHi)
{

    //Todo:
    // - same as calculateTailArray, bit set bits in bitmap
    // - check if bitmap is MAX (or 0 if 1->0) to check if tail can be advanced by 64/2=32
    return {0,0};
}

/// Should be thread safe (even if it is ring-buffer, since out of range intervals could never be produced by tail production)
Interval calculateTailArray(const std::vector<Interval>& intervals, const uint32_t largestHi) {
    // Todo: is a vector<bool> threadsafe?
    std::vector<int8_t> seenIntervals(largestHi * 2);
    seenIntervals[0] = 1;

    for (const auto& interval : intervals)
    {
        const auto firstIndex = (2 * interval.lo) - 1;
        seenIntervals[firstIndex] = 1;
        auto currentIndex = firstIndex + 1;
        for (size_t i = interval.lo + 1; i < interval.hi; ++i)
        {
            seenIntervals[currentIndex] = 1;
            ++currentIndex;
            seenIntervals[currentIndex] = 1;
            ++currentIndex;
        }
        seenIntervals[currentIndex] = 1;

    }
    const auto intervalSum = std::accumulate(seenIntervals.begin(), seenIntervals.end(), 0);
    const auto hi = (intervalSum + 1) / 2;

    return Interval{1, static_cast<uint32_t>(hi)};
}

std::pair<std::vector<Interval>, Interval> generateSpecificIntervalSequence()
{
    const std::vector<Interval> intervals{{9, 17}, {1,2}, {2,4}, {5,8}, {8,9}, {4,5}};
    return std::make_pair(intervals, Interval{1, 17});
}

int main()
{
    using namespace NES;

    const auto [intervals, maxInterval] = generateIntervals(200, /*seed*/ 1);
    // const auto [intervals, maxInterval] = generateSpecificIntervalSequence();
    for (const auto& [lo, hi] : intervals)
    {
        fmt::println("[{},{}]", lo, hi);
    }
    fmt::println("max interval: [{}-{}]", maxInterval.lo, maxInterval.hi);

    const auto finalInterval = calculateTailArray(intervals, maxInterval.hi);
    // const auto finalInterval = calculateTail(intervals, maxInterval.hi);
    fmt::println("Final interval: [{}-{}]", finalInterval.lo, finalInterval.hi);

    return 0;
}
