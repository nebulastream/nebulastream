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
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterOperatorHandler.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>


namespace NES::QueryCompilation
{

namespace
{
struct LoweringContext
{
    std::unordered_map<
        std::shared_ptr<Sinks::SinkDescriptor>,
        std::vector<std::variant<OriginId, std::weak_ptr<Runtime::Execution::ExecutablePipeline>>>>
        sinks;
    std::vector<Runtime::Execution::Source> sources;
    std::map<PipelineId, std::shared_ptr<Runtime::Execution::ExecutablePipeline>> pipelineToExecutableMap;
};

using Predecessor = std::variant<OriginId, std::weak_ptr<Runtime::Execution::ExecutablePipeline>>;
using Successor = std::optional<std::shared_ptr<Runtime::Execution::ExecutablePipeline>>;
std::shared_ptr<Runtime::Execution::ExecutablePipeline>
processOperatorPipeline(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context);
void processSink(const Predecessor& predecessor, const OperatorPipelinePtr& pipeline, LoweringContext& context);
Successor processSuccessor(
    const Predecessor& predecessor,
    const OperatorPipelinePtr& pipeline,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    LoweringContext& context);
Runtime::Execution::Source
processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context);

Successor processSuccessor(
    const Predecessor& predecessor,
    const OperatorPipelinePtr& pipeline,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    LoweringContext& context)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    if (pipeline->isSinkPipeline())
    {
        processSink(predecessor, pipeline, context);
        return {};
    }
    return processOperatorPipeline(pipeline, pipelineQueryPlan, context);
}

std::shared_ptr<Runtime::Execution::ExecutablePipeline> processInputFormatter(
    PipelineId pipelineId,
    QueryId queryId,
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successorPipelinesOfInputFormatter,
    const Runtime::NodeEnginePtr& nodeEngine,
    const Sources::SourceDescriptor& sourceDescriptor)
{
    auto inputFormatter = NES::InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceDescriptor.parserConfig.parserType,
        sourceDescriptor.schema,
        sourceDescriptor.parserConfig.tupleDelimiter,
        sourceDescriptor.parserConfig.fieldDelimiter);

    auto emitToSuccessorFunctionHandler
        = [successorPipelinesOfInputFormatter](Memory::TupleBuffer& buffer, Runtime::WorkerContextRef workerContext)
    {
        for (const auto& executableSuccessor : successorPipelinesOfInputFormatter)
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

    auto queryManager = nodeEngine->getQueryManager();
    auto emitToQueryManagerFunctionHandler = [successorPipelinesOfInputFormatter, queryManager](Memory::TupleBuffer& buffer)
    {
        for (const auto& executableSuccessor : successorPipelinesOfInputFormatter)
        {
            NES_TRACE("Emit buffer to query manager");
            queryManager->addWorkForNextPipeline(buffer, executableSuccessor);
        }
    };
    // Todo: create OperatorHandler for Parser
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers{std::make_shared<InputFormatterOperatorHandler>()};
    const auto executionContext = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
        pipelineId,
        queryId,
        queryManager->getBufferManager(),
        queryManager->getNumberOfWorkerThreads(),
        std::move(emitToSuccessorFunctionHandler),
        std::move(emitToQueryManagerFunctionHandler),
        operatorHandlers);

    // Todo:
    auto executableInputFormatterPipeline = Runtime::Execution::ExecutablePipeline::create(
        pipelineId,
        queryId,
        nodeEngine->getQueryManager(),
        executionContext,
        std::move(inputFormatter),
        1, //Todo: figure out
        std::move(successorPipelinesOfInputFormatter),
        false);
    return executableInputFormatterPipeline;
}

Runtime::Execution::Source
processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    // const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    // const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    // std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>> executableSuccessorPipelines;
    // for (const auto& successor : pipeline->getSuccessors())
    // {
    //     if (auto executableSuccessor = processSuccessor(sourceOperator->getOriginId(), successor, pipelineQueryPlan, context))
    //     {
    //         executableSuccessorPipelines.emplace_back(*executableSuccessor);
    //     }
    // }

    // context.sources.emplace_back(sourceOperator->getSourceDescriptor(), sourceOperator->getOriginId(), executableSuccessorPipelines);
    // return context.sources.back();

    /// Get successors of source (will become successors of InputFormatterPipeline
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successorPipelinesOfInputFormatter;
    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor
            = processSuccessor(successor, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
        successorPipelinesOfInputFormatter.emplace_back(executableSuccessor);
    }
    // Todo: not using pipelineToExecutableMap
    // -> we use pipelineToExecutableMap to detect whether we encountered a pipeline already

    /// Create executable pipeline from InputFormatter
    auto executableInputFormatterPipeline = processInputFormatter(
        pipeline->getPipelineId(),
        pipelineQueryPlan->getQueryId(),
        std::move(successorPipelinesOfInputFormatter),
        nodeEngine,
        sourceOperator->getSourceDescriptorRef());
    executablePipelines.emplace_back(executableInputFormatterPipeline);

    /// Set executable pipeline of InputFormatter as successor of source.
    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successorPipelinesOfSource;
    successorPipelinesOfSource.emplace_back(executableInputFormatterPipeline);

    /// Create emit function for source (that emits rawTB-InputFormatter-Task)
    auto sourceEmitFunction = nodeEngine->getQueryManager()->createSourceEmitFunction(std::move(successorPipelinesOfSource));

    /// Create source and emplace in sources.
    auto source = Sources::SourceProvider::lower(
        sourceOperator->getOriginId(),
        sourceOperator->getSourceDescriptorRef(),
        nodeEngine->getBufferManager(),
        std::move(sourceEmitFunction));
    sources.emplace_back(std::move(source));
}

void processSink(const Predecessor& predecessor, const OperatorPipelinePtr& pipeline, LoweringContext& context)
{
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rootOperator)->getSinkDescriptor();
    context.sinks[sinkOperator].emplace_back(predecessor);
}

std::shared_ptr<Runtime::Execution::ExecutablePipeline>
processOperatorPipeline(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context)
{
    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = context.pipelineToExecutableMap.find(pipeline->getPipelineId());
        executable != context.pipelineToExecutableMap.end())
    {
        return executable->second;
    }

    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto executableOperator = NES::Util::as<ExecutableOperator>(rootOperator);
    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(executableOperator->takeExecutablePipelineStage(), {});

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executablePipeline, successor, pipelineQueryPlan, context))
        {
            executablePipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    context.pipelineToExecutableMap.emplace(pipeline->getPipelineId(), executablePipeline);
    return executablePipeline;
}
}

Runtime::Execution::ExecutableQueryPlanPtr LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan)
{
    LoweringContext context;
    ///Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, pipelineQueryPlan, context);
    }

    std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>> pipelines;
    std::ranges::copy(std::views::values(context.pipelineToExecutableMap), std::back_inserter(pipelines));

    std::vector<Runtime::Execution::Sink> sinks;
    sinks.reserve(context.sinks.size());
    for (auto [k, v] : context.sinks)
    {
        sinks.emplace_back(k, std::move(v));
    }

    return Runtime::Execution::ExecutableQueryPlan::create(
        pipelineQueryPlan->getQueryId(), std::move(pipelines), std::move(sinks), std::move(context.sources));
}

}
