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
#include "../../../nes-client/include/API/Functions/Functions.hpp"
#include "../../../nes-execution/include/Execution/Operators/Scan.hpp"
#include "../../../nes-execution/include/Execution/Operators/Emit.hpp"
#include "../../../nes-execution/include/Execution/Operators/EmitOperatorHandler.hpp"
#include "../../../nes-execution/include/Execution/Operators/Selection.hpp"
#include "../../../nes-execution/include/Execution/Pipelines/PhysicalOperatorPipeline.hpp"
#include "../../../nes-execution/include/QueryCompiler/Operators/NautilusPipelineOperator.hpp"
#include "../../../nes-execution/include/QueryCompiler/Phases/Translations/FunctionProvider.hpp"
#include <ExecutablePipelineProviderRegistry.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include "../../../nes-functions/include/Functions/LogicalFunctions/NodeFunctionLess.hpp"
#include "../../../nes-memory/include/Util/TestTupleBuffer.hpp"
#include <Util/MicroBenchmarkUtils.hpp>
#include <Util/SleepOperator.hpp>

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
    const uint64_t numberOfTuplesPerTask,
    Runtime::WorkEmitter& emitter,
    Memory::AbstractBufferProvider& bufferProvider,
    Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
    std::chrono::microseconds sleepDurationPerTuple) const
{
    /// Defining the query and pipeline id
    constexpr QueryId queryId(1);
    constexpr PipelineId pipelineId(1);


    /// Creating here the executable pipeline for all tasks
    // auto pipelineStage = this->createFilterPipelineExecutableStage();
    // ((void) pipelineStage);
    auto pipelineStage = this->createSleepPipelineExecutableStage(sleepDurationPerTuple);
    pipelineStage->start(pipelineExecutionContext);
    auto runningQueryPlanNode = Runtime::RunningQueryPlanNode::create(
        queryId,
        pipelineId,
        emitter,
        {},
        std::move(pipelineStage),
        [](auto)
        {
        },
        Runtime::CallbackRef(),
        Runtime::CallbackRef());

    /// Filling all tuple buffers
    std::vector<Memory::TupleBuffer> allBuffers;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(minRange, maxRange);
    uint64_t ts = 0;
    for (uint64_t i = 0; i < numberOfTasks; i++)
    {
        auto tupleBuffer = bufferProvider.getBufferBlocking();
        Memory::MemoryLayouts::TestTupleBuffer testTupleBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(
            tupleBuffer,
            schemaInput);
        PRECONDITION(
            testTupleBuffer.getCapacity() >= numberOfTuplesPerTask,
            "The tuple buffer does not have enough capacity for the task.");
        for (uint64_t j = 0; j < numberOfTuplesPerTask; j++)
        {
            const uint64_t id = dis(gen);
            const uint64_t value = j;
            ts += 1;
            testTupleBuffer.pushRecordToBuffer(std::tuple<uint64_t, uint64_t, uint64_t>(id, value, ts));
            testTupleBuffer.setNumberOfTuples(j + 1);
        }
        Runtime::WorkTask task(
            queryId,
            pipelineId,
            runningQueryPlanNode,
            tupleBuffer,
            []()
            {
            },
            [](auto)
            {
            });
        taskQueue.write(std::move(task));

        // if (constexpr auto loggingInterval = 100; i % loggingInterval == 0)
        // {
        //     std::cout << "Created " << i << " tuple buffers of " << numberOfTasks << " with no. tuples " << tupleBuffer.getNumberOfTuples() << " and buffer size " << tupleBuffer.getBufferSize() << std::endl;
        // }
    }
    std::cout << "Created " << numberOfTasks << " tuple buffers with no. tuples " << numberOfTuplesPerTask << " totaling " << (numberOfTasks * numberOfTuplesPerTask) << " tuples with a buffer size of " << bufferSize << std::endl;

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
    const auto nautilusPipelineWrapper = std::make_shared<QueryCompilation::NautilusPipelineOperator>(
        operatorId,
        filterPipeline,
        operatorHandlers);

    /// Now creating an executable pipeline stage
    auto providerArguments = Runtime::Execution::ExecutablePipelineProviderRegistryArguments{};
    const auto provider = Runtime::Execution::ExecutablePipelineProviderRegistry::instance().create(providerName, providerArguments);
    nautilus::engine::Options options;
    auto pipelineStage
        = provider.value()->create(nautilusPipelineWrapper->getNautilusPipeline(), nautilusPipelineWrapper->getOperatorHandlers(), options);
    return pipelineStage;
}

std::unique_ptr<Runtime::Execution::ExecutablePipelineStage> MicroBenchmarkUtils::createSleepPipelineExecutableStage(std::chrono::microseconds sleepDurationPerTuple) const
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
    const auto nautilusPipelineWrapper = std::make_shared<QueryCompilation::NautilusPipelineOperator>(
        operatorId,
        filterPipeline,
        operatorHandlers);

    /// Now creating an executable pipeline stage
    auto providerArguments = Runtime::Execution::ExecutablePipelineProviderRegistryArguments{};
    const auto provider = Runtime::Execution::ExecutablePipelineProviderRegistry::instance().create(providerName, providerArguments);
    nautilus::engine::Options options;
    auto pipelineStage
        = provider.value()->create(nautilusPipelineWrapper->getNautilusPipeline(), nautilusPipelineWrapper->getOperatorHandlers(), options);
    return pipelineStage;
}

void BenchmarkWorkEmitter::emitWork(
    QueryId,
    const std::shared_ptr<Runtime::RunningQueryPlanNode>&,
    Memory::TupleBuffer,
    onComplete,
    onFailure)
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
