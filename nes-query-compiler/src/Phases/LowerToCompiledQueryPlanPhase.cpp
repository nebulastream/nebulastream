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

#include <Phases/LowerToCompiledQueryPlanPhase.hpp>

#include <memory>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Strings.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
#include <options.hpp>

namespace NES::QueryCompilation::LowerToCompiledQueryPlanPhase
{

namespace
{
struct LoweringContext
{
    std::unordered_map<std::shared_ptr<Sinks::SinkDescriptor>, std::vector<std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>>>
        sinks;
    std::vector<Source> sources;
    std::unordered_map<PipelineId, std::shared_ptr<ExecutablePipeline>> pipelineToExecutableMap;
};

/// forward declaring processOperatorPipeline() and processSink() to avoid a cyclic dependency between processSuccessor() and processOperatorPipeline()
using Predecessor = std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>;
using Successor = std::optional<std::shared_ptr<ExecutablePipeline>>;
std::shared_ptr<ExecutablePipeline> processOperatorPipeline(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);
void processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline, LoweringContext& loweringContext);
Successor processSuccessor(
    const Predecessor& predecessor,
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);
void processSource(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);

Successor processSuccessor(
    const Predecessor& predecessor,
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    if (pipeline->isSinkPipeline())
    {
        processSink(predecessor, pipeline, loweringContext);
        return {};
    }
    return processOperatorPipeline(pipeline, pipelineQueryPlan, loweringContext);
}

/// This functions injects a formatter in between sources and its successor pipelines.
/// The formatter is chosen based on the SourceDescriptor ParserConfig.
/// If the `raw` input formatter is used, no additional pipelines are injected.
/// The Return values of this function is the list of successors which the source should emit to.
std::vector<std::weak_ptr<ExecutablePipeline>> injectFormatter(
    const std::shared_ptr<Pipeline>& pipeline,
    const SourcePhysicalOperator& sourceOperator,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    const auto descriptor = sourceOperator.getDescriptor();
    if (NES::Util::toLowerCase(descriptor.getParserConfig().parserType) == "raw")
    {
        std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
        for (const auto& successor : pipeline->getSuccessors())
        {
            if (auto executableSuccessor = processSuccessor(sourceOperator.getOriginId(), successor, pipelineQueryPlan, loweringContext))
            {
                executableSuccessorPipelines.emplace_back(*executableSuccessor);
            }
        }
        return executableSuccessorPipelines;
    }

    /// Inject a formatter
    auto inputFormatter = NES::InputFormatters::InputFormatterProvider::provideInputFormatter(
        descriptor.getParserConfig().parserType,
        *descriptor.getLogicalSource().getSchema(),
        descriptor.getParserConfig().tupleDelimiter,
        descriptor.getParserConfig().fieldDelimiter);

    auto inputFormatterTask
        = std::make_unique<InputFormatters::InputFormatterTask>(sourceOperator.getOriginId(), std::move(inputFormatter));
    auto executableInputFormatterPipeline = ExecutablePipeline::create(pipeline->getPipelineId(), std::move(inputFormatterTask), {});

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executableInputFormatterPipeline, successor, pipelineQueryPlan, loweringContext))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }
    executableInputFormatterPipeline->successors = std::move(executableSuccessorPipelines);
    loweringContext.pipelineToExecutableMap.emplace(executableInputFormatterPipeline->id, executableInputFormatterPipeline);

    return {executableInputFormatterPipeline};
}

void processSource(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", *pipeline);

    /// Convert logical source descriptor to actual source descriptor
    const auto sourceOperator = pipeline->getRootOperator().get<SourcePhysicalOperator>();

    loweringContext.sources.emplace_back(
        sourceOperator.getOriginId(),
        sourceOperator.getDescriptor(),
        injectFormatter(pipeline, sourceOperator, pipelineQueryPlan, loweringContext));
}

void processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline, LoweringContext& loweringContext)
{
    const auto sinkOperator = pipeline->getRootOperator().get<SinkPhysicalOperator>().getDescriptor();
    loweringContext.sinks[sinkOperator].emplace_back(predecessor);
}

std::unique_ptr<ExecutablePipelineStage>
getStage(const std::shared_ptr<Pipeline>& pipeline, Nautilus::Configurations::ExecutionMode executionMode)
{
    nautilus::engine::Options options;
    switch (executionMode)
    {
        case Nautilus::Configurations::ExecutionMode::COMPILER: {
            options.setOption("engine.Compilation", true);
            break;
        }
        case Nautilus::Configurations::ExecutionMode::INTERPRETER: {
            options.setOption("engine.Compilation", false);
            break;
        }
        default: {
            INVARIANT(false, "Invalid backend");
        }
    }
    options.setOption("toConsole", true);
    options.setOption("toFile", true);

    return std::make_unique<CompiledExecutablePipelineStage>(pipeline, pipeline->getOperatorHandlers(), options);
}

std::shared_ptr<ExecutablePipeline> processOperatorPipeline(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = loweringContext.pipelineToExecutableMap.find(pipeline->getPipelineId());
        executable != loweringContext.pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    auto executablePipeline
        = ExecutablePipeline::create(PipelineId(pipeline->getPipelineId()), getStage(pipeline, pipelineQueryPlan->getExecutionMode()), {});

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executablePipeline, successor, pipelineQueryPlan, loweringContext))
        {
            executablePipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    loweringContext.pipelineToExecutableMap.emplace(pipeline->getPipelineId(), executablePipeline);
    return executablePipeline;
}
}

std::unique_ptr<CompiledQueryPlan> apply(const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan)
{
    LoweringContext loweringContext;
    ///Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, pipelineQueryPlan, loweringContext);
    }

    auto pipelines = std::move(loweringContext.pipelineToExecutableMap) | std::views::values | std::ranges::to<std::vector>();
    auto sinks = std::move(loweringContext.sinks)
        | std::views::transform([](auto descriptorAndPredecessors)
                                { return Sink(descriptorAndPredecessors.first, std::move(descriptorAndPredecessors.second)); })
        | std::ranges::to<std::vector>();

    return CompiledQueryPlan::create(
        pipelineQueryPlan->getQueryId(), std::move(pipelines), std::move(sinks), std::move(loweringContext.sources));
}

}
