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
            markAsSeen.begin(), markAsSeen.end(), static_cast<size_t>(0), [](size_t sum, const auto& atomicVal) { return sum + atomicVal; });
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

int main()
{
    using namespace NES;
    fmt::println("executing contention test");

    static constexpr size_t NUMBER_OF_TASKS = 1024 * 1024 * 2;
    static constexpr size_t SIZE_OF_BUFFERS = 1;
    static constexpr size_t NUMBER_OF_THREADS = 16;

    fmt::println("Size of TupleBuffer: {}", sizeof(Memory::TupleBuffer));
    fmt::println("Size of SampleStruct: {}", sizeof(SampleStruct));

    // const auto locklessExecutionTime = executeLockLess(NUMBER_OF_TASKS, SIZE_OF_BUFFERS, NUMBER_OF_THREADS);
    // const auto lockedExecutionTime = executeLocked(NUMBER_OF_TASKS, SIZE_OF_BUFFERS, NUMBER_OF_THREADS);
    // fmt::println("lockless processing time {}", locklessExecutionTime);
    // fmt::println("locked processing time {}", lockedExecutionTime);
    // fmt::println("ratio: {}", locklessExecutionTime / lockedExecutionTime);

    return 0;
}
