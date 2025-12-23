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

#include <algorithm>
#include <memory>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/DumpMode.hpp>
#include <Util/ExecutionMode.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <ExecutablePipelineStage.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
#include <options.hpp>

namespace NES
{

LowerToCompiledQueryPlanPhase::Successor
LowerToCompiledQueryPlanPhase::processSuccessor(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    if (pipeline->isSinkPipeline())
    {
        processSink(predecessor, pipeline);
        return {};
    }
    return processOperatorPipeline(pipeline);
}

void LowerToCompiledQueryPlanPhase::processSource(const std::shared_ptr<Pipeline>& pipeline)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", *pipeline);

    /// Convert logical source descriptor to actual source descriptor
    const auto sourceOperator = pipeline->getRootOperator().get<SourcePhysicalOperator>();

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(sourceOperator.id, successor))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }
    sources.emplace_back(
        sourceOperator.getOriginId(), sourceOperator.id, sourceOperator.getDescriptor(), std::move(executableSuccessorPipelines));
}

void LowerToCompiledQueryPlanPhase::processSink(const Predecessor& predecessor, const std::shared_ptr<Pipeline>& pipeline)
{
    const auto sinkOperator = pipeline->getRootOperator().get<SinkPhysicalOperator>().getDescriptor();
    auto it = std::ranges::find(sinks, pipeline->getPipelineId(), &CompiledQueryPlan::Sink::id);
    if (it == sinks.end())
    {
        sinks.emplace_back(pipeline->getPipelineId(), sinkOperator, std::vector<Predecessor>{});
        it = sinks.end() - 1;
    }
    it->predecessor.emplace_back(predecessor);
}

std::unique_ptr<ExecutablePipelineStage> LowerToCompiledQueryPlanPhase::getStage(const std::shared_ptr<Pipeline>& pipeline)
{
    nautilus::engine::Options options;
    /// We disable multithreading in MLIR by default to not interfere with NebulaStream's thread model
    options.setOption("mlir.enableMultithreading", false);
    switch (pipelineQueryPlan->getExecutionMode())
    {
        case ExecutionMode::COMPILER: {
            options.setOption("engine.Compilation", true);
            break;
        }
        case ExecutionMode::INTERPRETER: {
            options.setOption("engine.Compilation", false);
            break;
        }
        default: {
            INVARIANT(false, "Invalid backend");
        }
    }
    /// See: https://github.com/nebulastream/nautilus/blob/main/docs/options.md
    switch (dumpQueryCompilationIR.getDumpOption())
    {
        case DumpMode::Options::NONE:
            options.setOption("dump.all", false);
            options.setOption("dump.console", false);
            options.setOption("dump.file", false);
            break;
        case DumpMode::Options::CONSOLE:
            options.setOption("dump.all", true);
            options.setOption("dump.console", true);
            options.setOption("dump.file", false);
            break;
        case DumpMode::Options::FILE:
            options.setOption("dump.all", true);
            options.setOption("dump.console", false);
            options.setOption("dump.file", true);
            break;
        case DumpMode::Options::FILE_AND_CONSOLE:
            options.setOption("dump.all", true);
            options.setOption("dump.console", true);
            options.setOption("dump.file", true);
            break;
    }
    options.setOption("dump.graph", dumpQueryCompilationIR.isDumpGraphEnabled());
    return std::make_unique<CompiledExecutablePipelineStage>(pipeline, pipeline->getOperatorHandlers(), options);
}

std::shared_ptr<ExecutablePipeline> LowerToCompiledQueryPlanPhase::processOperatorPipeline(const std::shared_ptr<Pipeline>& pipeline)
{
    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = pipelineToExecutableMap.find(pipeline->getPipelineId()); executable != pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    auto executablePipeline = ExecutablePipeline::create(PipelineId(pipeline->getPipelineId()), getStage(pipeline), {});

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executablePipeline, successor))
        {
            executablePipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    pipelineToExecutableMap.emplace(pipeline->getPipelineId(), executablePipeline);
    return executablePipeline;
}

std::unique_ptr<CompiledQueryPlan> LowerToCompiledQueryPlanPhase::apply(const std::shared_ptr<PipelinedQueryPlan>& pipelineQueryPlan)
{
    this->pipelineQueryPlan = pipelineQueryPlan;

    /// Process all pipelines recursively.
    for (auto sourcePipelines = pipelineQueryPlan->getSourcePipelines(); const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline);
    }

    auto pipelines = std::move(pipelineToExecutableMap) | std::views::values | std::ranges::to<std::vector>();

    return CompiledQueryPlan::create(pipelineQueryPlan->getQueryId(), std::move(pipelines), std::move(sinks), std::move(sources));
}

}
