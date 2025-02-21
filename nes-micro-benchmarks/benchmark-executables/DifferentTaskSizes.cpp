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
#include <chrono>
#include <thread>

#include <benchmark/benchmark.h>
#include <folly/MPMCQueue.h>
#include <ErrorHandling.hpp>
#include "../../nes-memory/include/Runtime/BufferManager.hpp"
#include "../../nes-query-engine/Task.hpp"
#include "../../nes-query-engine/include/QueryEngineStatisticListener.hpp"
#include "../include/Util/MicroBenchmarkUtils.hpp"

namespace NES
{
static void DoSetup(const benchmark::State&)
{
}

static void DoTearDown(const benchmark::State&)
{
}

/// Expects the following arguments for this method:
/// 1. Number of tuples to be processed
/// 2. The number of tuples per task
/// 3. The sleep duration per tuple in microseconds
/// 4. The selectivity of the filter operator
/// 5. The index of the provider name in: {"INTERPRETER", "COMPILER"}
/// 6. The number of worker threads
/// 6. The number of different origin ids
[[maybe_unused]] static void BM_UniformPipeline(benchmark::State& state)
{
    /// Extracting the parameters from the state
    const auto numberOfTuples = state.range(0);
    auto numberOfTuplesPerTask = state.range(1);
    const auto sleepDurationPerTuple = std::chrono::nanoseconds(state.range(2));
    const auto selectivity = state.range(3);
    const auto providerName = std::array{"Interpreter"s, "Compilation"s}[state.range(4)];
    const auto numberOfWorkerThreads = state.range(5);
    const auto numberOfOrigins = state.range(6);

    /// Calculating the buffer size so that all tuples can be stored in it
    const auto schemaInput = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
        ->addField("id", DataTypeFactory::createUInt64())
        ->addField("value", DataTypeFactory::createUInt64())
        ->addField("ts", DataTypeFactory::createUInt64());
    const auto bufferSize = numberOfTuplesPerTask * schemaInput->getSchemaSizeInBytes();

    /// Creating a vector that contains the number of tuples per task for each task
    /// As we are running this experiment with a fixed number of tuples per task, we can create a vector with the same number of tuples per task
    /// This will produce a uniform distribution of tasks
    const auto numberOfTasks = numberOfTuples / numberOfTuplesPerTask;
    std::vector<uint64_t> numberOfTuplesPerTaskVector(numberOfTasks, numberOfTuplesPerTask);


    /// Creating some necessary variables for the benchmarking loop
    const auto schemaOutput = schemaInput->copy();
    const auto fieldNameForSelectivity = "id"s;
    const auto bufferProvider = Memory::BufferManager::create(bufferSize, numberOfTasks * 1.1 + 10);


    /// Setting the size of the task queue to be twice the number of tasks. This is to ensure that the task queue does not run out of space.
    folly::MPMCQueue<Runtime::WorkTask> taskQueue(numberOfTasks * 2);

    /// Creating all tasks for the benchmark
    BenchmarkWorkEmitter emitter;
    BenchmarkPEC pec(
        state.threads(),
        WorkerThreadId(state.thread_index()),
        PipelineId(1),
        bufferProvider,
        [&](const Memory::TupleBuffer&, auto)
        {
            /// No operation
        });

    const MicroBenchmarkUtils utils(selectivity, bufferSize, fieldNameForSelectivity, schemaInput, schemaOutput, providerName);
    const auto runningQueryPlanNode
        = utils.createTasks(
            taskQueue,
            numberOfTasks,
            numberOfTuplesPerTaskVector,
            emitter,
            *bufferProvider,
            pec,
            numberOfOrigins,
            sleepDurationPerTuple);

    /// Function for the worker threads to execute
    std::vector<uint64_t> countProcessedTuples(numberOfWorkerThreads, 0);
    struct TaskMeasurements
    {
        TaskMeasurements(
            SequenceNumber sequenceNumber,
            OriginId originId,
            const uint64_t numberOfInputTuples,
            const uint64_t startTime,
            const uint64_t endTime)
            : sequenceNumber(std::move(sequenceNumber))
            , originId(std::move(originId))
            , numberOfInputTuples(numberOfInputTuples)
            , startTime(startTime)
            , endTime(endTime)
        {
        }
        SequenceNumber sequenceNumber;
        OriginId originId;
        uint64_t numberOfInputTuples;
        uint64_t startTime;
        uint64_t endTime;
    };
    std::vector<std::vector<TaskMeasurements> > tupleBufferEmitStorage(numberOfWorkerThreads);
    auto workerFunction = [&](const WorkerThreadId threadId)
    {
        Runtime::WorkTask task;
        while (taskQueue.read(task))
        {
            if (auto pipeline = task.pipeline.lock())
            {
                BenchmarkPEC pec(
                    numberOfWorkerThreads,
                    threadId,
                    pipeline->id,
                    bufferProvider,
                    [&](const Memory::TupleBuffer&, auto)
                    {
                        /// We assume that the tuple buffer has a creation timestamp set.
                        const auto startTime = task.buf.getCreationTimestamp();
                        const auto endTime = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
                        const TaskMeasurements measurements{task.buf.getSequenceNumber(), task.buf.getOriginId(), task.buf.getNumberOfTuples(),
                                                            startTime.getRawValue(), static_cast<uint64_t>(endTime)};
                        tupleBufferEmitStorage[threadId.getRawValue()].emplace_back(measurements);
                    });
                pipeline->stage->execute(task.buf, pec);
                countProcessedTuples[threadId.getRawValue()] += task.buf.getNumberOfTuples();
            }
            else
            {
                INVARIANT(false, "Pipeline is not available for runningQueryPlanNode {}", runningQueryPlanNode->id);
            }
        }
    };

    /// Benchmarking the execution of the tasks
    for (auto _ : state)
    {
        /// Spawning all worker threads
        std::vector<std::thread> threads;
        threads.reserve(numberOfWorkerThreads);
        for (auto i = 0; i < numberOfWorkerThreads; i++)
        {
            threads.emplace_back(workerFunction, WorkerThreadId(i));
        }

        /// Waiting for all threads to finish
        for (auto& thread : threads)
        {
            thread.join();
        }
    }

    /// Combining all emitted buffers into a single vector
    std::vector<TaskMeasurements> emittedMeasurements;
    for (auto tasks : tupleBufferEmitStorage)
    {
        emittedMeasurements.insert(emittedMeasurements.end(), tasks.begin(), tasks.end());
    }

    /// Post processing the emitted measurements and then writing them to a csv file
    std::map<std::tuple<OriginId, uint64_t>, uint64_t> lastStartTime;
    std::vector<std::tuple<OriginId, uint64_t, uint64_t>> timeDiffData;

    // Sort measurements by OriginId, numberOfInputTuples, and startTime
    std::ranges::sort(emittedMeasurements, [](const TaskMeasurements& a, const TaskMeasurements& b) {
        return std::tie(a.originId, a.startTime) < std::tie(b.originId, b.startTime);
    });

    /// Calculate time differences
    for (const auto& measurement : emittedMeasurements) {
        auto key = std::make_tuple(measurement.originId, measurement.startTime);
        if (lastStartTime.contains(key)) {
            uint64_t timeDiff = measurement.startTime - lastStartTime[key];
            timeDiffData.emplace_back(measurement.originId, measurement.numberOfInputTuples, timeDiff);
        }
        lastStartTime[key] = measurement.startTime;
    }

    // Write to CSV file
    std::ofstream latencyFile("/tmp/latency_per_task_buffer.csv", std::ios::app);
    if (latencyFile.is_open()) {
        latencyFile << "OriginId,NumberOfInputTuples,TimeDifference,Selectivity,ProviderName,NumberOfWorkerThreads,Skewness\n";
        for (const auto& data : timeDiffData) {
            latencyFile << std::get<0>(data) << ","
                      << std::get<1>(data) << ","
                      << std::get<2>(data) << ","
            << selectivity << ","
            << providerName << ","
            << numberOfWorkerThreads << ","
            << 0.0 << "\n";
        }
        latencyFile.close();
    } else {
        std::cerr << "Unable to open file for writing." << std::endl;
    }


    /// Reporting additional statistics
    state.counters["Number_Of_Tasks"] = numberOfTasks;
    state.counters["Tuples_Processed"] = numberOfTasks * numberOfTuplesPerTask;
    for (auto i = 0; i < state.threads(); i++)
    {
        state.counters["Thread_" + std::to_string(i) + "_Processed_Tuples"] = countProcessedTuples[i];
    }
}

/// Expects the following arguments for this method:
/// 1. Number of tuples to be processed
/// 2. The number of tuples per task
/// 3. The sleep duration per tuple in microseconds
/// 4. The selectivity of the filter operator
/// 5. The index of the provider name in: {"INTERPRETER", "COMPILER"}
/// 6. The number of worker threads
/// 7. The skewness of the distribution of tasks
/// 8. Minimum number of tuples per task
[[maybe_unused]] static void BM_SkewedPipeline(benchmark::State& state)
{
    /// Extracting the parameters from the state
    const auto numberOfTuples = state.range(0);
    auto numberOfTuplesPerTask = state.range(1);
    const auto sleepDurationPerTuple = std::chrono::nanoseconds(state.range(2));
    const auto selectivity = state.range(3);
    const auto providerName = std::array{"Interpreter"s, "Compilation"s}[state.range(4)];
    const auto numberOfWorkerThreads = state.range(5);
    const auto skewness = state.range(6) / 100.0;
    const auto minNumberOfTuples = state.range(7);
    constexpr auto numberOfOrigins = 1;

    /// Calculating the buffer size so that all tuples can be stored in it
    const auto schemaInput = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
        ->addField("id", DataTypeFactory::createUInt64())
        ->addField("value", DataTypeFactory::createUInt64())
        ->addField("ts", DataTypeFactory::createUInt64());
    const auto bufferSize = numberOfTuplesPerTask * schemaInput->getSchemaSizeInBytes();

    /// Creating a vector that contains the number of tuples per task for each task.
    /// We would like to have a skewed distribution of tasks, where the first task has a lot of tuples and the last task has only a few tuples.
    /// This is to simulate a real-world scenario where the first task has a lot of tuples and the last task has only a few tuples.
    std::vector<uint64_t> numberOfTuplesPerTaskVector;
    int64_t remainingItems = numberOfTuples;
    const auto maxNumberOfTuples = bufferSize / schemaInput->getSchemaSizeInBytes();
    uint64_t rank = 1;
    constexpr auto incrementEvery = 3;
    uint64_t counter = 1;
    while (remainingItems > 0)
    {
        /// We must ensure that the skewed number of tuples sits between the min and max number of tuples
        const auto skewedNumberOfTuples = static_cast<int64_t>(maxNumberOfTuples * (1.0 / std::pow(rank, skewness)));
        const auto itemsInPackage = std::max(minNumberOfTuples, std::min(skewedNumberOfTuples, remainingItems));
        numberOfTuplesPerTaskVector.push_back(itemsInPackage);
        remainingItems -= itemsInPackage;
        ++counter;
        if (counter % incrementEvery == 0)
        {
            ++rank;
        }
    }

    const auto numberOfTasks = numberOfTuplesPerTaskVector.size();


    /// Creating some necessary variables for the benchmarking loop
    const auto schemaOutput = schemaInput->copy();
    const auto fieldNameForSelectivity = "id"s;
    const auto bufferProvider = Memory::BufferManager::create(bufferSize, numberOfTasks * 1.1 + 10);


    /// Setting the size of the task queue to be twice the number of tasks. This is to ensure that the task queue does not run out of space.
    folly::MPMCQueue<Runtime::WorkTask> taskQueue(numberOfTasks * 2);

    /// Creating all tasks for the benchmark
    BenchmarkWorkEmitter emitter;
    BenchmarkPEC pec(
        state.threads(),
        WorkerThreadId(state.thread_index()),
        PipelineId(1),
        bufferProvider,
        [&](const Memory::TupleBuffer&, auto)
        {
            /// No operation
        });

    const MicroBenchmarkUtils utils(selectivity, bufferSize, fieldNameForSelectivity, schemaInput, schemaOutput, providerName);
    const auto runningQueryPlanNode
        = utils.createTasks(
            taskQueue,
            numberOfTasks,
            numberOfTuplesPerTaskVector,
            emitter,
            *bufferProvider,
            pec,
            numberOfOrigins,
            sleepDurationPerTuple);

    /// Function for the worker threads to execute
    std::vector<uint64_t> countProcessedTuples(numberOfWorkerThreads, 0);

    struct TaskMeasurements
    {
        TaskMeasurements(SequenceNumber sequenceNumber, uint64_t numberOfInputTuples, uint64_t latencyInUs)
            : sequenceNumber(sequenceNumber)
            , numberOfInputTuples(numberOfInputTuples)
            , latencyInUs(latencyInUs)
        {
        }
        SequenceNumber sequenceNumber;
        uint64_t numberOfInputTuples;
        uint64_t latencyInUs;
    };
    std::vector<std::vector<TaskMeasurements> > tupleBufferEmitStorage(numberOfWorkerThreads);
    auto workerFunction = [&](const WorkerThreadId threadId)
    {
        Runtime::WorkTask task;
        while (taskQueue.read(task))
        {
            if (auto pipeline = task.pipeline.lock())
            {
                BenchmarkPEC pec(
                    numberOfWorkerThreads,
                    threadId,
                    pipeline->id,
                    bufferProvider,
                    [&](const Memory::TupleBuffer& buffer, auto)
                    {
                        /// Calculating the duration in milliseconds. We assume that the tuple buffer has a creation timestamp set.
                        /// We subtract the current time from the creation timestamp to get the duration and write this back to the tuple buffer.
                        const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::system_clock::now().time_since_epoch() - std::chrono::microseconds(
                                buffer.getCreationTimestamp().getRawValue()));
                        const TaskMeasurements measurements{task.buf.getSequenceNumber(), buffer.getNumberOfTuples(),
                                                            static_cast<uint64_t>(duration.count())};
                        tupleBufferEmitStorage[threadId.getRawValue()].emplace_back(measurements);
                    });
                pipeline->stage->execute(task.buf, pec);
                countProcessedTuples[threadId.getRawValue()] += task.buf.getNumberOfTuples();
            }
            else
            {
                INVARIANT(false, "Pipeline is not available for runningQueryPlanNode {}", runningQueryPlanNode->id);
            }
        }
    };

    /// Benchmarking the execution of the tasks
    for (auto _ : state)
    {
        /// Spawning all worker threads
        std::vector<std::thread> threads;
        threads.reserve(numberOfWorkerThreads);
        for (auto i = 0; i < numberOfWorkerThreads; i++)
        {
            threads.emplace_back(workerFunction, WorkerThreadId(i));
        }

        /// Waiting for all threads to finish
        for (auto& thread : threads)
        {
            thread.join();
        }
    }

    /// Combining all emitted buffers into a single vector
    std::vector<TaskMeasurements> emittedMeasurements;
    for (auto tasks : tupleBufferEmitStorage)
    {
        emittedMeasurements.insert(emittedMeasurements.end(), tasks.begin(), tasks.end());
    }

    /// Now we can write the latency per task buffer to a csv file, such that we can analyze the latency of each task buffer.
    /// Additionally, we would like to write the other parameters to the csv file as well.
    std::ofstream latencyFile("/tmp/latency_per_task_buffer.csv", std::ios::app);
    latencyFile << "SequenceNumber,LatencyInUS,NumberOfTuplesInput,Selectivity,ProviderName,NumberOfWorkerThreads,Skewness\n";
    for (auto& taskMeasurement : emittedMeasurements)
    {
        const auto latency = taskMeasurement.latencyInUs;
        const auto numberOfTuplesInput = numberOfTuplesPerTaskVector[taskMeasurement.sequenceNumber.getRawValue() -
            SequenceNumber::INITIAL];
        latencyFile << taskMeasurement.sequenceNumber << "," << latency << "," << numberOfTuplesInput << "," << selectivity << "," <<
            providerName << ","
            << numberOfWorkerThreads << "," << skewness << "\n";
    }


    /// Reporting additional statistics
    state.counters["Number_Of_Tasks"] = numberOfTasks;
    state.counters["Tuples_Processed"] = numberOfTasks * numberOfTuplesPerTask;
    for (auto i = 0; i < state.threads(); i++)
    {
        state.counters["Thread_" + std::to_string(i) + "_Processed_Tuples"] = countProcessedTuples[i];
    }
}
}

constexpr auto NUM_REPETITIONS = 3;

/// Registering all benchmark functions
BENCHMARK(NES::BM_UniformPipeline)
     ->ArgsProduct(
    {
        {10 * 1000}, /// Number of tuples that should be processed
        {1000, 3000}, //{1, 10, 100, 500, 1000, 2000, 3000}, /// Number of tuples per task/buffer.
        {100}, /// Sleep duration per tuple in nanoseconds
        {10, 50, 90}, //benchmark::CreateDenseRange(0, 100, 10), /// Selectivity
        {1}, /// Provider name: 0 -> INTERPRETER, 1 -> COMPILER
        {8}, //benchmark::CreateRange(1, 16, 2) /// Number of worker threads
        {2} /// Number of origins
    })
     ->Setup(NES::DoSetup)
     ->Teardown(NES::DoTearDown)
     ->UseRealTime()
     ->Unit(benchmark::kMillisecond)
     ->Iterations(1)
     ->Repetitions(NUM_REPETITIONS);

// BENCHMARK(NES::BM_SkewedPipeline)
//     ->ArgsProduct(
//     {
//         {100 * 1000}, /// Number of tuples that should be processed
//         {1000}, /// Max number of tuples per task/buffer.
//         {100}, /// Sleep duration per tuple in nanoseconds
//         {10, 50, 90}, /// Selectivity
//         {1}, /// Provider name: 0 -> INTERPRETER, 1 -> COMPILER
//         {1, 4, 8, 16}, ///benchmark::CreateRange(1, 16, 2), /// Number of worker threads
//         {1, 20, 50, 55, 60, 70, 100}, /// Skewness (will be divided by 100 to achieve a double value)
//         {10} /// Minimum number of tuples per task (should be set in accordance to the max number of tuples per task)
//     })
//     ->Setup(NES::DoSetup)
//     ->Teardown(NES::DoTearDown)
//     ->UseRealTime()
//     ->Unit(benchmark::kMillisecond)
//     ->Iterations(1)
//     ->Repetitions(NUM_REPETITIONS);

BENCHMARK_MAIN();