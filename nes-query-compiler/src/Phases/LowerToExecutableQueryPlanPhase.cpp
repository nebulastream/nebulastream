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
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Common.hpp>
#include <ExecutableOperator.hpp>
#include <ExecutableQueryPlan.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Phases/LowerToExecutableQueryPlanPhase.hpp>

namespace NES::QueryCompilation::LowerToExecutableQueryPlanPhase
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

using Predecessor = std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>;
using Successor = std::optional<std::shared_ptr<ExecutablePipeline>>;
std::shared_ptr<ExecutablePipeline> processOperatorPipeline(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);


/// forward declarations
Source
processSource(std::shared_ptr<Pipeline> pipeline, std::shared_ptr<PipelinedQueryPlan> pipelineQueryPlan, LoweringContext loweringContext);
void processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline, LoweringContext& loweringContext);
std::shared_ptr<ExecutablePipeline> processPipeline(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);

Successor processSuccessor(
    const Predecessor& predecessor,
    std::shared_ptr<Pipeline> pipeline,
    std::shared_ptr<PipelinedQueryPlan> pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    ///PRECONDITION(pipeline->isSinkPipeline() || pipeline->isPipeline(), "expected a Sink or Pipeline");

    if (Util::instanceOf<SinkPipeline>(pipeline))
    {
        processSink(predecessor, pipeline, loweringContext);
        return {};
    }
    return processPipeline(pipeline, pipelineQueryPlan, loweringContext);
}

Source
processSource(std::shared_ptr<Pipeline> pipeline, std::shared_ptr<PipelinedQueryPlan> pipelineQueryPlan, LoweringContext loweringContext)
{
    /// PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    ///const auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    const auto sourcePipeline = NES::Util::as<SourcePipeline>(pipeline);
    const auto sourceOperator = sourcePipeline->sourceOperator;

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->successorPipelines)
    {
        if (auto executableSuccessor = processSuccessor(sourceOperator->getOriginId(), successor, pipelineQueryPlan, loweringContext))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }

    loweringContext.sources.emplace_back(sourceOperator->getOriginId(), sourceOperator->getSourceDescriptor(), executableSuccessorPipelines);
    return loweringContext.sources.back();
}

void processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline, LoweringContext& loweringContext)
{
    const auto sinkPipeline = NES::Util::as<SinkPipeline>(pipeline);
    const auto sinkOperatorDescriptor = sinkPipeline->sinkOperator->getSinkDescriptor();
    loweringContext.sinks[sinkOperatorDescriptor].emplace_back(predecessor);
}

std::shared_ptr<ExecutablePipeline> processPipeline(
    const std::shared_ptr<Pipeline>& pipeline,
    const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    /// PRECONDITION(!pipeline->getQueryPlan()->getRootOperators().empty(), "A pipeline should have at least one root operator");

    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = loweringContext.pipelineToExecutableMap.find(pipeline->id);
        executable != loweringContext.pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    // const auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    const auto rootOperator = Util::as<OperatorPipeline>(pipeline)->operators[0];
    const auto executableOperator = NES::Util::as<ExecutableOperator>(rootOperator);
    auto executablePipeline = ExecutablePipeline::create(PipelineId(pipeline->id), executableOperator->takeStage(), {});
    for (const auto& successor : pipeline->successorPipelines)
    {
        if (auto executableSuccessor = processSuccessor(executablePipeline, successor, pipelineQueryPlan, loweringContext))
        {
            executablePipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    loweringContext.pipelineToExecutableMap.emplace(pipeline->id, executablePipeline);
    return executablePipeline;
}


std::unique_ptr<CompiledQueryPlan> LowerToExecutableQueryPlanPhase::apply(const std::unique_ptr<PipelinedQueryPlan> pipelineQueryPlan)
{
    LoweringContext loweringContext;

    /// Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, pipelineQueryPlan, loweringContext);
    }

    auto pipelines = std::move(loweringContext.pipelineToExecutableMap) | std::views::values | std::ranges::to<std::vector>();
    auto sinks = std::move(loweringContext.sinks)
        | std::views::transform(
                     [](auto descriptorAndPredecessors)
                     { return Sink(descriptorAndPredecessors.first, std::move(descriptorAndPredecessors.second)); })
        | std::ranges::to<std::vector>();

    return CompiledQueryPlan::create(
        pipelineQueryPlan->getQueryId(), std::move(pipelines), std::move(sinks), std::move(loweringContext.sources));
}

}
