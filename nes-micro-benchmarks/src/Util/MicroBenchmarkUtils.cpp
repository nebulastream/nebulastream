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

#include <random>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Util/MicroBenchmarkUtils.hpp>
#include <Util/SleepOperator.hpp>
#include <ExecutablePipelineProviderRegistry.hpp>
#include "../../../nes-client/include/API/Functions/Functions.hpp"
#include "../../../nes-common/include/Sequencing/SequenceData.hpp"
#include "../../../nes-execution/include/Execution/Operators/Emit.hpp"
#include "../../../nes-execution/include/Execution/Operators/EmitOperatorHandler.hpp"
#include "../../../nes-execution/include/Execution/Operators/Scan.hpp"
#include "../../../nes-execution/include/Execution/Operators/Selection.hpp"
#include "../../../nes-execution/include/Execution/Pipelines/PhysicalOperatorPipeline.hpp"
#include "../../../nes-execution/include/QueryCompiler/Operators/NautilusPipelineOperator.hpp"
#include "../../../nes-execution/include/QueryCompiler/Phases/Translations/FunctionProvider.hpp"
#include "../../../nes-functions/include/Functions/LogicalFunctions/NodeFunctionLess.hpp"
#include "../../../nes-memory/include/Util/TestTupleBuffer.hpp"

namespace NES
{
MicroBenchmarkUtils::MicroBenchmarkUtils(
    const uint64_t selectivity,
    const uint64_t bufferSize,
    std::string fieldNameForSelectivity,
    std::shared_ptr<Schema> schemaInput,
    std::shared_ptr<Schema> schemaOutput,
    std::string providerName)
    : selectivity(selectivity)
    , bufferSize(bufferSize)
    , fieldNameForSelectivity(std::move(fieldNameForSelectivity))
    , schemaInput(std::move(schemaInput))
    , schemaOutput(std::move(schemaOutput))
    , providerName(std::move(providerName))
{
}

std::shared_ptr<Runtime::RunningQueryPlanNode> MicroBenchmarkUtils::createTasks(
    folly::MPMCQueue<Runtime::WorkTask>& taskQueue,
    const uint64_t numberOfTasks,
    const std::vector<uint64_t>& numberOfTuplesPerTask,
    Runtime::WorkEmitter& emitter,
    Memory::AbstractBufferProvider& bufferProvider,
    Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
    std::chrono::nanoseconds sleepDurationPerTuple) const
{
    PRECONDITION(numberOfTuplesPerTask.size() == numberOfTasks, "The number of tasks and the number of tuples per task do not match.");

    /// Defining the query and pipeline id
    constexpr QueryId queryId(1);
    constexpr PipelineId pipelineId(1);


    /// Creating here the executable pipeline for all tasks
    auto pipelineStage = this->createFilterPipelineExecutableStage();
    ((void)sleepDurationPerTuple);
    // auto pipelineStage = this->createSleepPipelineExecutableStage(sleepDurationPerTuple);
    pipelineStage->start(pipelineExecutionContext);
    auto runningQueryPlanNode = Runtime::RunningQueryPlanNode::create(
        queryId, pipelineId, emitter, {}, std::move(pipelineStage), [](auto) {}, Runtime::CallbackRef(), Runtime::CallbackRef());

    struct InfoForThread
    {
        InfoForThread() : sequenceNumber(SequenceNumber::INVALID), startTimestamp(0), numberOfTuples(0) { }
        InfoForThread(SequenceNumber sequenceNumber, const uint64_t startTimestamp, const uint64_t numberOfTuples)
            : sequenceNumber(std::move(sequenceNumber)), startTimestamp(startTimestamp), numberOfTuples(numberOfTuples)
        {
        }
        SequenceNumber sequenceNumber;
        uint64_t startTimestamp;
        uint64_t numberOfTuples;
    };
    folly::MPMCQueue<InfoForThread> infoForThreads(numberOfTasks);

    /// First, we are creating the necessary information for the threads and emplacing it into the queue
    /// Thus, we can then afterwards use the stored information to create tuple buffers concurrently
    SequenceNumber::Underlying seqNumber = SequenceNumber::INITIAL;
    uint64_t ts = 0;
    for (const auto tuplesPerTask : numberOfTuplesPerTask)
    {
        InfoForThread infoForThread{SequenceNumber(seqNumber), ts, tuplesPerTask};
        infoForThreads.write(std::move(infoForThread));
        seqNumber += 1;
        ts += tuplesPerTask;
    }

    auto populateTupleBufferFunction = [&taskQueue, &infoForThreads, &runningQueryPlanNode, this, &bufferProvider, &queryId, &pipelineId]()
    {
        InfoForThread infoForThread;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint64_t> dis(minRange, maxRange);
        while (infoForThreads.read(infoForThread))
        {
            auto tupleBuffer = bufferProvider.getBufferBlocking();
            tupleBuffer.setChunkNumber(ChunkNumber(1));
            tupleBuffer.setLastChunk(true);
            tupleBuffer.setSequenceNumber(infoForThread.sequenceNumber);
            tupleBuffer.setCreationTimestamp(Runtime::Timestamp(
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count()));
            tupleBuffer.setWatermark(tupleBuffer.getCreationTimestampInMS());

            Memory::MemoryLayouts::TestTupleBuffer testTupleBuffer
                = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schemaInput);
            PRECONDITION(
                testTupleBuffer.getCapacity() >= infoForThread.numberOfTuples,
                "The tuple buffer does not have enough capacity for the task of {}",
                infoForThread.numberOfTuples);
            for (uint64_t j = 0; j < infoForThread.numberOfTuples; j++)
            {
                const uint64_t id = dis(gen);
                const uint64_t value = j;
                testTupleBuffer.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, uint64_t>(id, value, infoForThread.startTimestamp + j));
                testTupleBuffer.setNumberOfTuples(j + 1);
            }
            Runtime::WorkTask task(queryId, pipelineId, runningQueryPlanNode, tupleBuffer, []() {}, [](auto) {});
            taskQueue.write(std::move(task));
        }
    };

    const auto startTime = std::chrono::high_resolution_clock::now();
    /// Spawning all threads
    const auto numberOfWorkerThreads = std::thread::hardware_concurrency() - 2;
    std::vector<std::thread> threads;
    threads.reserve(numberOfWorkerThreads);
    for (auto i = 0UL; i < numberOfWorkerThreads; i++)
    {
        threads.emplace_back(populateTupleBufferFunction);
    }

    /// Waiting for all threads to finish
    for (auto& thread : threads)
    {
        thread.join();
    }
    const auto endTime = std::chrono::high_resolution_clock::now();
    const auto duration = endTime - startTime;

    std::cout << "Created " << numberOfTasks << " tuple buffers with a buffer size of " << bufferSize << std::endl;
    std::cout << "Taskqueue has " << taskQueue.size() << " elements" << std::endl;
    std::cout << "Took a total of " << std::chrono::duration_cast<std::chrono::seconds>(duration).count() << " s" << " with "
              << numberOfWorkerThreads << " threads" << std::endl;

    return runningQueryPlanNode;
}

std::unique_ptr<Runtime::Execution::ExecutablePipelineStage> MicroBenchmarkUtils::createFilterPipelineExecutableStage() const
{
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>> operatorHandlers;

    /// Creating the filter operator fieldNameForSelectivity < selectivity
    const auto filterPredicate = NodeFunctionLess::create(
        NodeFunctionFieldAccess::create(fieldNameForSelectivity),
        NodeFunctionConstantValue::create(DataTypeFactory::createUInt64(), std::to_string(selectivity * 10)));
    auto filterFunction = QueryCompilation::FunctionProvider::lowerFunction(filterPredicate);
    const auto filterOperator = std::make_shared<Runtime::Execution::Operators::Selection>(std::move(filterFunction));

    /// Creating the scan operator
    const auto memoryProviderInput = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferSize, schemaInput);
    const auto scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(memoryProviderInput, schemaInput->getFieldNames());

    /// Creating the emit operator
    const auto memoryProviderOutput = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferSize, schemaOutput);
    operatorHandlers.push_back(std::make_unique<Runtime::Execution::Operators::EmitOperatorHandler>());
    const auto emitOperator = std::make_shared<Runtime::Execution::Operators::Emit>(operatorHandlers.size() - 1, memoryProviderOutput);


    /// Building the pipeline together
    auto filterPipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    filterPipeline->setRootOperator(scanOperator);
    scanOperator->setChild(filterOperator);
    filterOperator->setChild(emitOperator);
    constexpr OperatorId operatorId(1);
    const auto nautilusPipelineWrapper
        = std::make_shared<QueryCompilation::NautilusPipelineOperator>(operatorId, filterPipeline, operatorHandlers);

    /// Now creating an executable pipeline stage
    auto providerArguments = Runtime::Execution::ExecutablePipelineProviderRegistryArguments{};
    const auto provider = Runtime::Execution::ExecutablePipelineProviderRegistry::instance().create(providerName, providerArguments);
    nautilus::engine::Options options;
    auto pipelineStage
        = provider.value()->create(nautilusPipelineWrapper->getNautilusPipeline(), nautilusPipelineWrapper->getOperatorHandlers(), options);
    return pipelineStage;
}

std::unique_ptr<Runtime::Execution::ExecutablePipelineStage>
MicroBenchmarkUtils::createSleepPipelineExecutableStage(std::chrono::nanoseconds sleepDurationPerTuple) const
{
    std::vector<std::shared_ptr<Runtime::Execution::OperatorHandler>> operatorHandlers;

    /// Creating the sleep operator
    const auto sleepOperator = std::make_shared<Runtime::Execution::Operators::SleepOperator>(sleepDurationPerTuple);

    /// Creating the scan operator
    const auto memoryProviderInput = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferSize, schemaInput);
    const auto scanOperator = std::make_shared<Runtime::Execution::Operators::Scan>(memoryProviderInput, schemaInput->getFieldNames());

    /// Creating the emit operator
    const auto memoryProviderOutput = Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferSize, schemaOutput);
    operatorHandlers.push_back(std::make_unique<Runtime::Execution::Operators::EmitOperatorHandler>());
    const auto emitOperator = std::make_shared<Runtime::Execution::Operators::Emit>(operatorHandlers.size() - 1, memoryProviderOutput);


    /// Building the pipeline together
    auto filterPipeline = std::make_shared<Runtime::Execution::PhysicalOperatorPipeline>();
    filterPipeline->setRootOperator(scanOperator);
    scanOperator->setChild(sleepOperator);
    sleepOperator->setChild(emitOperator);
    constexpr OperatorId operatorId(1);
    const auto nautilusPipelineWrapper
        = std::make_shared<QueryCompilation::NautilusPipelineOperator>(operatorId, filterPipeline, operatorHandlers);

    /// Now creating an executable pipeline stage
    auto providerArguments = Runtime::Execution::ExecutablePipelineProviderRegistryArguments{};
    const auto provider = Runtime::Execution::ExecutablePipelineProviderRegistry::instance().create(providerName, providerArguments);
    nautilus::engine::Options options;
    auto pipelineStage
        = provider.value()->create(nautilusPipelineWrapper->getNautilusPipeline(), nautilusPipelineWrapper->getOperatorHandlers(), options);
    return pipelineStage;
}

void BenchmarkWorkEmitter::emitWork(
    QueryId, const std::shared_ptr<Runtime::RunningQueryPlanNode>&, Memory::TupleBuffer, onComplete, onFailure)
{
}
void BenchmarkWorkEmitter::emitPipelineStart(QueryId, const std::shared_ptr<Runtime::RunningQueryPlanNode>&, onComplete, onFailure)
{
}

void BenchmarkWorkEmitter::emitPendingPipelineStop(QueryId, std::shared_ptr<Runtime::RunningQueryPlanNode>, onComplete, onFailure)
{
}
void BenchmarkWorkEmitter::emitPipelineStop(QueryId, std::unique_ptr<Runtime::RunningQueryPlanNode>, onComplete, onFailure)
{
}
}
