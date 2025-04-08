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
#include <Identifiers/Identifiers.hpp>
#include <Phases/LowerToExecutableQueryPlanPhase.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineProviderRegistry.hpp>
#include <ExecutableQueryPlan.hpp>
#include <Pipeline.hpp>
#include <options.hpp>

namespace NES::QueryCompilation::LowerToExecutableQueryPlanPhase
{

namespace
{
struct LoweringContext
{
    std::unordered_map<
        std::shared_ptr<Sinks::SinkDescriptor>,
        std::vector<std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>>>
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
    const auto sourceOperator =  pipeline->rootOperator.get<SourcePhysicalOperator>();

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->successorPipelines)
    {
        if (auto executableSuccessor = processSuccessor(sourceOperator.getOriginId(), successor, pipelineQueryPlan, loweringContext))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }

    loweringContext.sources.emplace_back(
        sourceOperator.getOriginId(), sourceOperator.getDescriptor(), executableSuccessorPipelines);
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
    options.setOption("toConsole", true);
    options.setOption("toFile", true);

    auto providerArguments = ExecutablePipelineProviderRegistryArguments{};
    const auto provider = ExecutablePipelineProviderRegistry::instance().create(pipeline->getProviderType(), providerArguments);
    INVARIANT(provider, "Cannot find ExecutablePipelineProvider");
    return  provider.value()->create(pipeline, pipeline->operatorHandlers, options);
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
    auto executablePipeline
        = ExecutablePipeline::create(PipelineId(pipeline->pipelineId), getStage(pipeline), {});

    for (const auto& successor : pipeline->successorPipelines)
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

std::unique_ptr<CompiledQueryPlan>
LowerToExecutableQueryPlanPhase::apply(const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan)
{
    LoweringContext loweringContext;
    ///Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, pipelineQueryPlan, loweringContext);
    }

    auto tempPipelineMap = std::move(loweringContext.pipelineToExecutableMap);
    std::vector<decltype(tempPipelineMap.begin()->second)> pipelines;
    pipelines.reserve(tempPipelineMap.size());
    for (auto& entry : tempPipelineMap) {
        pipelines.push_back(std::move(entry.second));
    }

    auto tempSinks = std::move(loweringContext.sinks);
    std::vector<Sink> sinks;
    sinks.reserve(tempSinks.size());
    for (auto& descriptorAndPredecessors : tempSinks) {
        sinks.push_back(Sink(descriptorAndPredecessors.first, std::move(descriptorAndPredecessors.second)));
    }

    return CompiledQueryPlan::create(
        pipelineQueryPlan->queryId, std::move(pipelines), std::move(sinks), std::move(loweringContext.sources));
}

}
