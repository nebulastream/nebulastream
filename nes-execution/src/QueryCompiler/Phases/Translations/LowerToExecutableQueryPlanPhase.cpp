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
#include <ranges>
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
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Executable.hpp>
#include <magic_enum.hpp>


namespace NES::QueryCompilation
{
using namespace NES::Runtime::Execution;

struct LoweringContext
{
    std::vector<Sink> sinks;
    std::vector<Source> sources;
    std::map<PipelineId, std::shared_ptr<ExecutablePipeline>> pipelineToExecutableMap;
};

using Predecessor = std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>;
using Successor = std::variant<std::shared_ptr<ExecutablePipeline>, Sink>;

static std::shared_ptr<ExecutablePipeline>
processOperatorPipeline(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context);
static Sink processSink(Predecessor predecessor, const OperatorPipelinePtr& pipeline, LoweringContext& context);
static Successor processSuccessor(
    Predecessor predecessor, const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context);
static Source processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context);


ExecutableQueryPlanPtr LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan)
{
    LoweringContext context;
    ///Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, pipelineQueryPlan, context);
    }

    std::vector<std::shared_ptr<ExecutablePipeline>> pipelines;
    std::ranges::copy(std::views::values(context.pipelineToExecutableMap), std::back_inserter(pipelines));
    return ExecutableQueryPlan::create(std::move(pipelines), std::move(context.sinks), std::move(context.sources));
}

static Successor processSuccessor(
    Predecessor predecessor, const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    if (pipeline->isSinkPipeline())
    {
        return processSink(predecessor, pipeline, context);
    }
    return processOperatorPipeline(pipeline, pipelineQueryPlan, context);
}

static Source processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& context)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor = processSuccessor(sourceOperator->getOriginId(), successor, pipelineQueryPlan, context);
        if (auto* successorPipeline = std::get_if<std::shared_ptr<ExecutablePipeline>>(&executableSuccessor))
        {
            executableSuccessorPipelines.emplace_back(*successorPipeline);
        }
    }

    context.sources.emplace_back(sourceOperator->getSourceDescriptor(), sourceOperator->getOriginId(), executableSuccessorPipelines);
    return context.sources.back();
}

static Sink processSink(Predecessor predecessor, const OperatorPipelinePtr& pipeline, LoweringContext& context)
{
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rootOperator)->getSinkDescriptor();
    context.sinks.emplace_back(sinkOperator, predecessor);
    return context.sinks.back();
}

static std::shared_ptr<ExecutablePipeline>
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
    auto executablePipeline = ExecutablePipeline::create(executableOperator->takeExecutablePipelineStage(), {});

    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor = processSuccessor(executablePipeline, successor, pipelineQueryPlan, context);
        if (auto* successorPipeline = std::get_if<std::shared_ptr<ExecutablePipeline>>(&executableSuccessor))
        {
            executablePipeline->successors.emplace_back(*successorPipeline);
        }
    }

    context.pipelineToExecutableMap.emplace(pipeline->getPipelineId(), executablePipeline);
    return executablePipeline;
}

}
