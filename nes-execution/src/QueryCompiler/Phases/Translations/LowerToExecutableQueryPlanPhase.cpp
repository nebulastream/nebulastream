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
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Ranges.hpp>
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
    std::unordered_map<PipelineId, std::shared_ptr<Runtime::Execution::ExecutablePipeline>> pipelineToExecutableMap;
};

/// forward declaring processOperatorPipeline() and processSink() to avoid a cyclic dependency between processSuccessor() and processOperatorPipeline()
using Predecessor = std::variant<OriginId, std::weak_ptr<Runtime::Execution::ExecutablePipeline>>;
using Successor = std::optional<std::shared_ptr<Runtime::Execution::ExecutablePipeline>>;
std::shared_ptr<Runtime::Execution::ExecutablePipeline> processOperatorPipeline(
    const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& loweringContext);
void processSink(const Predecessor& predecessor, const OperatorPipelinePtr& pipeline, LoweringContext& loweringContext);
Successor processSuccessor(
    const Predecessor& predecessor,
    const OperatorPipelinePtr& pipeline,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    LoweringContext& loweringContext);
Runtime::Execution::Source
processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& loweringContext);

Successor processSuccessor(
    const Predecessor& predecessor,
    const OperatorPipelinePtr& pipeline,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
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

Runtime::Execution::Source
processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(sourceOperator->getOriginId(), successor, pipelineQueryPlan, loweringContext))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }

    loweringContext.sources.emplace_back(
        sourceOperator->getOriginId(), sourceOperator->getSourceDescriptor(), executableSuccessorPipelines);
    return loweringContext.sources.back();
}

void processSink(const Predecessor& predecessor, const OperatorPipelinePtr& pipeline, LoweringContext& loweringContext)
{
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rootOperator)->getSinkDescriptor();
    loweringContext.sinks[sinkOperator].emplace_back(predecessor);
}

std::shared_ptr<Runtime::Execution::ExecutablePipeline> processOperatorPipeline(
    const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& loweringContext)
{
    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = loweringContext.pipelineToExecutableMap.find(pipeline->getPipelineId());
        executable != loweringContext.pipelineToExecutableMap.end())
    {
        return executable->second;
    }

    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto executableOperator = NES::Util::as<ExecutableOperator>(rootOperator);
    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(executableOperator->takeStage(), {});

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

Runtime::Execution::ExecutableQueryPlanPtr LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan)
{
    LoweringContext loweringContext;
    ///Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, pipelineQueryPlan, loweringContext);
    }

    auto pipelines = std::move(loweringContext.pipelineToExecutableMap) | std::views::values | ranges::to<std::vector>();
    auto sinks = std::move(loweringContext.sinks)
        | std::views::transform(
                     [](auto descriptorAndPredecessors)
                     { return Runtime::Execution::Sink(descriptorAndPredecessors.first, std::move(descriptorAndPredecessors.second)); })
        | ranges::to<std::vector>();

    return Runtime::Execution::ExecutableQueryPlan::create(std::move(pipelines), std::move(sinks), std::move(loweringContext.sources));
}

}
