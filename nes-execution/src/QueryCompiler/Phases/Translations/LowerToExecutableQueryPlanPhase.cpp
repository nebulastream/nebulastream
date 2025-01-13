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

#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>


namespace NES::QueryCompilation
{

Runtime::Execution::ExecutableQueryPlanPtr
LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan, const Runtime::NodeEnginePtr& nodeEngine)
{
    std::vector<std::unique_ptr<Sources::SourceHandle>> sources;
    std::unordered_set<std::shared_ptr<NES::Sinks::Sink>> sinks;
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
    std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
    std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = pipelineToExecutableMap.find(pipeline->getPipelineId()); executable != pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    if (pipeline->isSinkPipeline())
    {
        auto executableSink = processSink(pipeline, sinks, pipelineQueryPlan->getQueryId());
        pipelineToExecutableMap.insert({pipeline->getPipelineId(), executableSink});
        return executableSink;
    }
    /// if it is an OperatorPipeline
    auto executablePipeline
        = processOperatorPipeline(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
    pipelineToExecutableMap.insert({pipeline->getPipelineId(), executablePipeline});
    return executablePipeline;
}

void LowerToExecutableQueryPlanPhase::processSource(
    const OperatorPipelinePtr& pipeline,
    std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
    std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor
            = processSuccessor(successor, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }
    auto emitFunction = nodeEngine->getQueryManager()->createSourceEmitFunction(std::move(executableSuccessorPipelines));
    auto source = Sources::SourceProvider::lower(
        sourceOperator->getOriginId(),
        sourceOperator->getSourceDescriptorRef(),
        nodeEngine->getBufferManager(),
        std::move(emitFunction));
    sources.emplace_back(std::move(source));
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processSink(
    const OperatorPipelinePtr& pipeline, std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks, QueryId queryId)
{
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rootOperator);
    /// Todo #34 (ls-1801 & alepping): As soon as the QueryManager stores sinks as pipelines that become tasks, we can return unique_ptrs.
    /// Right now, we store a shared_ptr to use the sink as a task, and to later call sink->open() in QueryManagerLifecycle::registerQuery
    sinkOperator->sinkDescriptor->schema = sinkOperator->getOutputSchema();
    auto sinkSharedPtr = Sinks::SinkProvider::lower(queryId, sinkOperator->getSinkDescriptorRef());
    sinks.emplace(sinkSharedPtr);
    return sinkSharedPtr;
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processOperatorPipeline(
    const OperatorPipelinePtr& pipeline,
    std::vector<std::unique_ptr<Sources::SourceHandle>>& sources,
    std::unordered_set<std::shared_ptr<NES::Sinks::Sink>>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap)
{
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto executableOperator = NES::Util::as<ExecutableOperator>(rootOperator);

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
            if (const auto sink = std::get_if<std::shared_ptr<NES::Sinks::Sink>>(&executableSuccessor))
            {
                NES_TRACE("Emit Buffer to data sink {}", **sink);
                (*sink)->emitTupleBuffer(buffer);
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

    const auto executionContext = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
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

}
