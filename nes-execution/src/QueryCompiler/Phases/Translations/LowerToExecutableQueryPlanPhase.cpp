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
    PipelineId::Underlying pipelineIdGenerator = PipelineId::INITIAL;
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

Runtime::Execution::Source
processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(sourceOperator->getOriginId(), successor, pipelineQueryPlan, context))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }

    context.sources.emplace_back(sourceOperator->getSourceDescriptor(), sourceOperator->getOriginId(), executableSuccessorPipelines);
    return context.sources.back();
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
    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(
        PipelineId(context.pipelineIdGenerator++), executableOperator->takeExecutablePipelineStage(), {});

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
