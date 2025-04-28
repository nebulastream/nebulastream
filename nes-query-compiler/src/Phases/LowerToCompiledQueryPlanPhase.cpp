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

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Phases/LowerToCompiledQueryPlanPhase.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Pipeline.hpp>
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
Source processSource(
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

Source processSource(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto sourceOperator = pipeline->rootOperator.get<SourcePhysicalOperator>();

    std::vector<std::shared_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    auto inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceOperator.getDescriptor()->parserConfig.parserType,
        sourceOperator.getDescriptor()->schema,
        sourceOperator.getDescriptor()->parserConfig.tupleDelimiter,
        sourceOperator.getDescriptor()->parserConfig.fieldDelimiter);
    auto inputFormatterTask
        = std::make_unique<InputFormatters::InputFormatterTask>(sourceOperator.getOriginId(), std::move(inputFormatter));

    auto executableInputFormatterPipeline = ExecutablePipeline::create(
    pipeline->pipelineId, std::move(inputFormatterTask), executableSuccessorPipelines);

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executableInputFormatterPipeline, successor, pipelineQueryPlan, loweringContext))
        {
            executableInputFormatterPipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    /// Insert the executable pipeline into the pipelineQueryPlan at position 1 (after the source)
    pipelineQueryPlan->removePipeline(pipeline);

    std::vector<std::weak_ptr<ExecutablePipeline>> inputFormatterTasks;

    loweringContext.pipelineToExecutableMap.emplace(getNextPipelineId(), executableInputFormatterPipeline);
    inputFormatterTasks.emplace_back(executableInputFormatterPipeline);

    loweringContext.sources.emplace_back(sourceOperator.getOriginId(), sourceOperator.getDescriptor(), std::move(inputFormatterTasks));
    return loweringContext.sources.back();
}

void processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline, LoweringContext& loweringContext)
{
    const auto sinkOperator = pipeline->rootOperator.get<SinkPhysicalOperator>().getDescriptor();
    loweringContext.sinks[sinkOperator].emplace_back(predecessor);
}

std::unique_ptr<ExecutablePipelineStage> getStage(const std::shared_ptr<Pipeline>& pipeline)
{
    nautilus::engine::Options options;
    switch (pipeline->providerType)
    {
        case Pipeline::ProviderType::Compiler: {
            options.setOption("engine.Compilation", true);
            break;
        }
        case Pipeline::ProviderType::Interpreter: {
            options.setOption("engine.Compilation", false);
            break;
        }
        default: {
            INVARIANT(false, "Invalid backend");
        }
    }
    options.setOption("toConsole", true);
    options.setOption("toFile", true);

    return std::make_unique<CompiledExecutablePipelineStage>(pipeline, pipeline->operatorHandlers, options);
}

std::shared_ptr<ExecutablePipeline> processOperatorPipeline(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = loweringContext.pipelineToExecutableMap.find(pipeline->pipelineId);
        executable != loweringContext.pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    auto executablePipeline = ExecutablePipeline::create(PipelineId(pipeline->pipelineId), getStage(pipeline), {});

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executablePipeline, successor, pipelineQueryPlan, loweringContext))
        {
            executablePipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    loweringContext.pipelineToExecutableMap.emplace(pipeline->pipelineId, executablePipeline);
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
        pipelineQueryPlan->queryId, std::move(pipelines), std::move(sinks), std::move(loweringContext.sources));
}

}
