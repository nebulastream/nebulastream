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
#include <variant>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Executable.hpp>
#include <magic_enum.hpp>


namespace NES::QueryCompilation
{
using namespace NES::Runtime::Execution;
LowerToExecutableQueryPlanPhase::LowerToExecutableQueryPlanPhase(DataSinkProviderPtr sinkProvider)
    : sinkProvider(std::move(sinkProvider)) {};

std::shared_ptr<LowerToExecutableQueryPlanPhase> LowerToExecutableQueryPlanPhase::create(const DataSinkProviderPtr& sinkProvider)
{
    return std::make_shared<LowerToExecutableQueryPlanPhase>(sinkProvider);
}

ExecutableQueryPlanPtr LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan)
{
    std::vector<std::tuple<OriginId, std::shared_ptr<NES::Sources::SourceDescriptor>, std::vector<std::weak_ptr<ExecutablePipeline>>>>
        sources;
    std::vector<std::shared_ptr<ExecutablePipeline>> executablePipelines;
    std::map<PipelineId, std::shared_ptr<ExecutablePipeline>> pipelineToExecutableMap;
    ///Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines)
    {
        processSource(pipeline, sources, executablePipelines, pipelineQueryPlan, pipelineToExecutableMap);
    }
    return ExecutableQueryPlan::create(executablePipelines, std::move(sources));
}
std::shared_ptr<ExecutablePipeline> LowerToExecutableQueryPlanPhase::processSuccessor(
    const OperatorPipelinePtr& pipeline,
    std::vector<std::tuple<OriginId, std::shared_ptr<NES::Sources::SourceDescriptor>, std::vector<std::weak_ptr<ExecutablePipeline>>>>&
        sources,
    std::vector<std::shared_ptr<ExecutablePipeline>>& executablePipelines,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, std::shared_ptr<ExecutablePipeline>>& pipelineToExecutableMap)
{
    PRECONDITION(pipeline->isSinkPipeline() || pipeline->isOperatorPipeline(), "expected a Sink or OperatorPipeline");

    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = pipelineToExecutableMap.find(pipeline->getPipelineId()); executable != pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    if (pipeline->isSinkPipeline())
    {
        auto executableSink = processSink(pipeline, sources, executablePipelines, pipelineQueryPlan);
        pipelineToExecutableMap.insert({pipeline->getPipelineId(), executableSink});
        return executableSink;
    }
    /// if it is an OperatorPipeline
    auto executablePipeline = processOperatorPipeline(pipeline, sources, executablePipelines, pipelineQueryPlan, pipelineToExecutableMap);
    pipelineToExecutableMap.insert({pipeline->getPipelineId(), executablePipeline});
    return executablePipeline;
}

void LowerToExecutableQueryPlanPhase::processSource(
    const OperatorPipelinePtr& pipeline,
    Sources& sources,
    std::vector<std::shared_ptr<ExecutablePipeline>>& executablePipelines,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, std::shared_ptr<ExecutablePipeline>>& pipelineToExecutableMap)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor = processSuccessor(successor, sources, executablePipelines, pipelineQueryPlan, pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }
    sources.emplace_back(sourceOperator->getOriginId(), sourceOperator->getSourceDescriptor(), executableSuccessorPipelines);
}

std::shared_ptr<ExecutablePipeline> LowerToExecutableQueryPlanPhase::processSink(
    const OperatorPipelinePtr& pipeline,
    std::vector<std::tuple<OriginId, std::shared_ptr<NES::Sources::SourceDescriptor>, std::vector<std::weak_ptr<ExecutablePipeline>>>>&,
    std::vector<std::shared_ptr<ExecutablePipeline>>& pipelines,
    const PipelineQueryPlanPtr& pipelineQueryPlan)
{
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto sinkOperator = NES::Util::as<PhysicalOperators::PhysicalSinkOperator>(rootOperator);
    auto numOfProducers = pipeline->getPredecessors().size();
    auto sink = sinkProvider->lower(
        sinkOperator->getId(), sinkOperator->getSinkDescriptor(), sinkOperator->getOutputSchema(), pipelineQueryPlan, numOfProducers);

    auto sinkPipeline = std::make_shared<ExecutablePipeline>(std::move(sink), std::vector<std::shared_ptr<ExecutablePipeline>>{});
    pipelines.emplace_back(sinkPipeline);
    return sinkPipeline;
}

std::shared_ptr<ExecutablePipeline> LowerToExecutableQueryPlanPhase::processOperatorPipeline(
    const OperatorPipelinePtr& pipeline,
    std::vector<std::tuple<OriginId, std::shared_ptr<NES::Sources::SourceDescriptor>, std::vector<std::weak_ptr<ExecutablePipeline>>>>&
        sources,
    std::vector<std::shared_ptr<ExecutablePipeline>>& executablePipelines,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<PipelineId, std::shared_ptr<ExecutablePipeline>>& pipelineToExecutableMap)
{
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto executableOperator = NES::Util::as<ExecutableOperator>(rootOperator);

    std::vector<std::shared_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors())
    {
        auto executableSuccessor = processSuccessor(successor, sources, executablePipelines, pipelineQueryPlan, pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto executablePipeline
        = std::make_shared<ExecutablePipeline>(executableOperator->takeExecutablePipelineStage(), executableSuccessorPipelines);

    executablePipelines.emplace_back(executablePipeline);
    return executablePipeline;
}

}
