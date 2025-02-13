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
/// 5. The buffer size
/// 6. The index of the provider name in: {"INTERPRETER", "COMPILER"}
/// 7. The number of worker threads
static void BM_SleepPipeline(benchmark::State& state)
{
    /// Extracting the parameters from the state
    const auto numberOfTuples = state.range(0);
    auto numberOfTuplesPerTask = state.range(1);
    const auto sleepDurationPerTuple = std::chrono::milliseconds(state.range(2));
    const auto selectivity = state.range(3);
    const auto bufferSize = state.range(4);
    const auto providerName = std::array{"Interpreter"s, "Compilation"s}[state.range(5)];
    const auto numberOfWorkerThreads = state.range(6);


    /// Creating some necessary variables for the benchmarking loop
    const auto schemaInput = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
        ->addField("id", DataTypeFactory::createUInt64())
        ->addField("value", DataTypeFactory::createUInt64())
        ->addField("ts", DataTypeFactory::createUInt64());
    const auto schemaOutput = schemaInput->copy();
    const auto fieldNameForSelectivity = "id"s;
    const auto maxNumberOfTuples = bufferSize / schemaInput->getSchemaSizeInBytes();
    // numberOfTuplesPerTask = numberOfTuplesPerTask == 0 ? maxNumberOfTuples : numberOfTuplesPerTask;
    const auto numberOfTasks = numberOfTuples / numberOfTuplesPerTask;
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
    const auto runningQueryPlanNode = utils.createTasks(taskQueue, numberOfTasks, numberOfTuplesPerTask, emitter, *bufferProvider, pec, sleepDurationPerTuple);

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
                    [&](const Memory::TupleBuffer&, auto)
                    {
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

    /// Reporting additional statistics
    state.counters["Number_Of_Tasks"] = numberOfTasks;
    state.counters["Tuples_Processed"] = numberOfTasks * numberOfTuplesPerTask;
    for (auto i = 0; i < state.threads(); i++)
    {
        state.counters["Thread_" + std::to_string(i) + "_Processed_Tuples"] = countProcessedTuples[i];
    }
}
}

/// Registering all benchmark functions
BENCHMARK(NES::BM_SleepPipeline)
    ->ArgsProduct(
    {
        {10 * 1000}, /// Number of tuples that should be processed
        benchmark::CreateDenseRange(25, 400, 25), /// Number of tuples per task.
        {1}, /// Sleep duration per tuple in microseconds
        {10}, /// Selectivity
        {16 * 1024}, /// Buffer size
        {1}, /// Provider name: 0 -> INTERPRETER, 1 -> COMPILER
        benchmark::CreateRange(1, 8, 2) /// Number of worker threads
    })
    ->Setup(NES::DoSetup)
    ->Teardown(NES::DoTearDown)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond)
    ->Repetitions(10);

BENCHMARK_MAIN();