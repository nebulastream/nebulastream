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
[[maybe_unused]] static void BM_UniformPipeline(benchmark::State& state)
{
    /// Extracting the parameters from the state
    const auto numberOfTuples = state.range(0);
    auto numberOfTuplesPerTask = state.range(1);
    const auto sleepDurationPerTuple = std::chrono::nanoseconds(state.range(2));
    const auto selectivity = state.range(3);
    const auto providerName = std::array{"Interpreter"s, "Compilation"s}[state.range(4)];
    const auto numberOfWorkerThreads = state.range(5);

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
        = utils.createTasks(taskQueue, numberOfTasks, numberOfTuplesPerTaskVector, emitter, *bufferProvider, pec, sleepDurationPerTuple);

    /// Function for the worker threads to execute
    std::vector<uint64_t> countProcessedTuples(numberOfWorkerThreads, 0);
    auto workerFunction = [&](const WorkerThreadId threadId)
    {
        Runtime::WorkTask task;
        while (taskQueue.read(task))
        {
            if (auto pipeline = task.pipeline.lock())
            {
                BenchmarkPEC pec(
                    state.threads(),
                    WorkerThreadId(state.thread_index()),
                    pipeline->id,
                    bufferProvider,
                    [&](const Memory::TupleBuffer&, auto) {});
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
static void BM_SkewedPipeline(benchmark::State& state)
{
    /// Extracting the parameters from the state
    const auto numberOfTuples = state.range(0);
    auto numberOfTuplesPerTask = state.range(1);
    const auto sleepDurationPerTuple = std::chrono::nanoseconds(state.range(2));
    const auto selectivity = state.range(3);
    const auto providerName = std::array{"Interpreter"s, "Compilation"s}[state.range(4)];
    const auto numberOfWorkerThreads = state.range(5);
    const auto skewness = state.range(6) / 100.0;

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
    uint64_t remainingItems = numberOfTuples;
    const auto maxNumberOfItems = bufferSize / schemaInput->getSchemaSizeInBytes();
    uint64_t rank = 1;
    while (remainingItems > 0)
    {
        const auto itemsInPackage = std::min(static_cast<uint64_t>(maxNumberOfItems * (1.0 / std::pow(rank, skewness))), remainingItems);
        numberOfTuplesPerTaskVector.push_back(itemsInPackage);
        remainingItems -= itemsInPackage;
        rank++;
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
        = utils.createTasks(taskQueue, numberOfTasks, numberOfTuplesPerTaskVector, emitter, *bufferProvider, pec, sleepDurationPerTuple);

    /// Function for the worker threads to execute
    std::vector<uint64_t> countProcessedTuples(numberOfWorkerThreads, 0);
    auto workerFunction = [&](const WorkerThreadId threadId)
    {
        Runtime::WorkTask task;
        while (taskQueue.read(task))
        {
            if (auto pipeline = task.pipeline.lock())
            {
                BenchmarkPEC pec(
                    state.threads(),
                    WorkerThreadId(state.thread_index()),
                    pipeline->id,
                    bufferProvider,
                    [&](const Memory::TupleBuffer&, auto) {});
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
// BENCHMARK(NES::BM_UniformPipeline)
//      ->ArgsProduct({
//          {100 * 1000 * 1000}, /// Number of tuples that should be processed
//          {1, 10, 100, 500, 1000, 2000, 3000}, /// Number of tuples per task/buffer.
//          {100}, /// Sleep duration per tuple in nanoseconds
//          benchmark::CreateDenseRange(0, 100, 10), /// Selectivity
//          {1}, /// Provider name: 0 -> INTERPRETER, 1 -> COMPILER
//          benchmark::CreateRange(1, 16, 2) /// Number of worker threads
//      })
//      ->Setup(NES::DoSetup)
//      ->Teardown(NES::DoTearDown)
//      ->UseRealTime()
//      ->Unit(benchmark::kMillisecond)
//      ->Iterations(1)
//      ->Repetitions(NUM_REPETITIONS);

BENCHMARK(NES::BM_SkewedPipeline)
    ->ArgsProduct({
        {100 * 1000 * 1000}, /// Number of tuples that should be processed
        {1000}, /// Max number of tuples per task/buffer.
        {100}, /// Sleep duration per tuple in nanoseconds
        benchmark::CreateDenseRange(0, 100, 10), /// Selectivity
        {1}, /// Provider name: 0 -> INTERPRETER, 1 -> COMPILER
        benchmark::CreateRange(1, 16, 2), /// Number of worker threads
        {1, 100, 150, 200} /// Skewness (will be divided by 100 to achieve a double value)
    })
    ->Setup(NES::DoSetup)
    ->Teardown(NES::DoTearDown)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->Repetitions(NUM_REPETITIONS);

BENCHMARK_MAIN();
