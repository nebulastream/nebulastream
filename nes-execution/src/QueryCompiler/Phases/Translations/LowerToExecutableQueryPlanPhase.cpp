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

#include <variant>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES::QueryCompilation
{
LowerToExecutableQueryPlanPhase::LowerToExecutableQueryPlanPhase(
    DataSinkProviderPtr sinkProvider, Sources::DataSourceProviderPtr sourceProvider)
    : sinkProvider(std::move(sinkProvider)), sourceProvider(std::move(sourceProvider)) {};

LowerToExecutableQueryPlanPhasePtr
LowerToExecutableQueryPlanPhase::create(const DataSinkProviderPtr& sinkProvider, const Sources::DataSourceProviderPtr& sourceProvider)
{
    return std::make_shared<LowerToExecutableQueryPlanPhase>(sinkProvider, sourceProvider);
}

Runtime::Execution::ExecutableQueryPlanPtr
LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan, const Runtime::NodeEnginePtr& nodeEngine)
{
    std::vector<Sources::SourceHandlePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<Runtime::Execution::ExecutablePipelinePtr> executablePipelines;
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline> pipelineToExecutableMap;
    ///Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
    }

    return std::make_shared<Runtime::Execution::ExecutableQueryPlan>(
        pipelineQueryPlan->getQueryId(),
        std::move(sources),
        std::move(sinks),
        std::move(executablePipelines),
        nodeEngine->getQueryManager(),
        nodeEngine->getBufferManager());
}
Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processSuccessor(
    const OperatorPipelinePtr& pipeline,
    std::vector<Sources::SourceHandlePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    /// check if the particular pipeline already exist in the pipeline map.
    if (pipelineToExecutableMap.find(pipeline->getPipelineId()) != pipelineToExecutableMap.end())
    {
        return pipelineToExecutableMap.at(pipeline->getPipelineId());
    }

    if (pipeline->isSinkPipeline())
    {
        auto executableSink = processSink(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan);
        pipelineToExecutableMap.insert({pipeline->getPipelineId(), executableSink});
        return executableSink;
    }
    else /// if it is an OperatorPipeline
    {
        auto executablePipeline = processOperatorPipeline(
            pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
        pipelineToExecutableMap.insert({pipeline->getPipelineId(), executablePipeline});
        return executablePipeline;
    }
}

void LowerToExecutableQueryPlanPhase::processSource(
    const OperatorPipelinePtr& pipeline,
    std::vector<Sources::SourceHandlePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline " + pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sourceOperator = rootOperator->as<PhysicalOperators::PhysicalSourceOperator>();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    PRECONDITION(!sourceDescriptor->instanceOf<LogicalSourceDescriptor>(), "logical source name lookup is not supported");

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor
            = processSuccessor(successor, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto emitFunction = nodeEngine->getQueryManager()->createSourceEmitFunction(std::move(executableSuccessorPipelines));
    auto source = sourceProvider->lower(
        sourceOperator->getOriginId(), std::move(sourceDescriptor), nodeEngine->getBufferManager(), std::move(emitFunction));
    sources.emplace_back(source);
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processSink(
    const OperatorPipelinePtr& pipeline,
    std::vector<Sources::SourceHandlePtr>&,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>&,
    Runtime::NodeEnginePtr nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan)
{
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sinkOperator = rootOperator->as<PhysicalOperators::PhysicalSinkOperator>();
    auto numOfProducers = pipeline->getPredecessors().size();
    auto sink = sinkProvider->lower(
        sinkOperator->getId(),
        sinkOperator->getSinkDescriptor(),
        sinkOperator->getOutputSchema(),
        std::move(nodeEngine),
        pipelineQueryPlan,
        numOfProducers);
    sinks.emplace_back(sink);
    return sink;
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processOperatorPipeline(
    const OperatorPipelinePtr& pipeline,
    std::vector<Sources::SourceHandlePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap)
{
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto executableOperator = rootOperator->as<ExecutableOperator>();

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor
            = processSuccessor(successor, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto queryManager = nodeEngine->getQueryManager();

    auto emitToSuccessorFunctionHandler
        = [executableSuccessorPipelines](Memory::TupleBuffer& buffer, Runtime::WorkerContextRef workerContext)
    {
        for (const auto& executableSuccessor : executableSuccessorPipelines)
        {
            if (const auto* sink = std::get_if<DataSinkPtr>(&executableSuccessor))
            {
                NES_TRACE("Emit Buffer to data sink {}", (*sink)->toString());
                (*sink)->writeData(buffer, workerContext);
            }
            else if (const auto* nextExecutablePipeline = std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor))
            {
                NES_TRACE("Emit Buffer to pipeline {}", (*nextExecutablePipeline)->getPipelineId());
                (*nextExecutablePipeline)->execute(buffer, workerContext);
            }
        }
    };

    auto emitToQueryManagerFunctionHandler = [executableSuccessorPipelines, queryManager](Memory::TupleBuffer& buffer)
    {
        for (const auto& executableSuccessor : executableSuccessorPipelines)
        {
            NES_TRACE("Emit buffer to query manager");
            queryManager->addWorkForNextPipeline(buffer, executableSuccessor);
        }
    };

    auto executionContext = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
        pipeline->getPipelineId(),
        pipelineQueryPlan->getQueryId(),
        queryManager->getBufferManager(),
        queryManager->getNumberOfWorkerThreads(),
        emitToSuccessorFunctionHandler,
        emitToQueryManagerFunctionHandler,
        executableOperator->getOperatorHandlers());

    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(
        pipeline->getPipelineId(),
        pipelineQueryPlan->getQueryId(),
        queryManager,
        executionContext,
        executableOperator->getExecutablePipelineStage(),
        pipeline->getPredecessors().size(),
        executableSuccessorPipelines);

    executablePipelines.emplace_back(executablePipeline);
    return executablePipeline;
}

SourceDescriptorPtr LowerToExecutableQueryPlanPhase::createSourceDescriptor(SchemaPtr schema, PhysicalSourceTypePtr physicalSourceType)
{
    auto logicalSourceName = physicalSourceType->getLogicalSourceName();
    auto physicalSourceName = physicalSourceType->getPhysicalSourceName();
    auto sourceType = physicalSourceType->getSourceType();
    NES_DEBUG(
        "PhysicalSourceConfig: create Actual source descriptor with physical source: {} {} ",
        physicalSourceType->toString(),
        magic_enum::enum_name(sourceType));

    switch (sourceType)
    {
        case SourceType::CSV_SOURCE: {
            auto csvSourceType = physicalSourceType->as<CSVSourceType>();
            return CSVSourceDescriptor::create(schema, csvSourceType, logicalSourceName, physicalSourceName);
        }
        case SourceType::TCP_SOURCE: {
            auto tcpSourceType = physicalSourceType->as<TCPSourceType>();
            return TCPSourceDescriptor::create(schema, tcpSourceType, logicalSourceName, physicalSourceName);
        }
        default: {
            throw UnknownSourceType(physicalSourceType->getSourceTypeAsString());
        }
    }
}

} /// namespace NES::QueryCompilation
