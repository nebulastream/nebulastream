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
#include <InputFormatters/InputFormatterTask.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Ranges.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>

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
    const std::shared_ptr<OperatorPipeline>& pipeline,
    const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);
void processSink(const Predecessor& predecessor, const std::shared_ptr<OperatorPipeline>& pipeline, LoweringContext& loweringContext);
Successor processSuccessor(
    const Predecessor& predecessor,
    const std::shared_ptr<OperatorPipeline>& pipeline,
    const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);
Runtime::Execution::Source processSource(
    const std::shared_ptr<OperatorPipeline>& pipeline,
    const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext);

Successor processSuccessor(
    const Predecessor& predecessor,
    const std::shared_ptr<OperatorPipeline>& pipeline,
    const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan,
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

Runtime::Execution::Source processSource(
    const std::shared_ptr<OperatorPipeline>& pipeline,
    const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators().front();
    const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>> executableSuccessorPipelines;
    auto inputFormatter = NES::InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceOperator->getSourceDescriptorRef().parserConfig.parserType,
        *sourceOperator->getSourceDescriptorRef().schema,
        sourceOperator->getSourceDescriptorRef().parserConfig.tupleDelimiter,
        sourceOperator->getSourceDescriptorRef().parserConfig.fieldDelimiter);
    auto inputFormatterTask
        = std::make_unique<InputFormatters::InputFormatterTask>(sourceOperator->getOriginId(), std::move(inputFormatter));

    auto executableInputFormatterPipeline = Runtime::Execution::ExecutablePipeline::create(
        pipeline->getPipelineId(), std::move(inputFormatterTask), executableSuccessorPipelines);

    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(executableInputFormatterPipeline, successor, pipelineQueryPlan, loweringContext))
        {
            executableInputFormatterPipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    /// Insert the executable pipeline into the pipelineQueryPlan at position 1 (after the source)
    pipelineQueryPlan->removePipeline(pipeline);

    std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>> inputFormatterTasks;

    loweringContext.pipelineToExecutableMap.emplace(getNextPipelineId(), executableInputFormatterPipeline);
    inputFormatterTasks.emplace_back(executableInputFormatterPipeline);

    loweringContext.sources.emplace_back(
        sourceOperator->getOriginId(), sourceOperator->getSourceDescriptor(), std::move(inputFormatterTasks));
    return loweringContext.sources.back();
}

void processSink(const Predecessor& predecessor, const std::shared_ptr<OperatorPipeline>& pipeline, LoweringContext& loweringContext)
{
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sinkOperator = NES::Util::as<SinkLogicalOperator>(rootOperator)->getSinkDescriptor();
    loweringContext.sinks[sinkOperator].emplace_back(predecessor);
}

std::shared_ptr<Runtime::Execution::ExecutablePipeline> processOperatorPipeline(
    const std::shared_ptr<OperatorPipeline>& pipeline,
    const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(!pipeline->getDecomposedQueryPlan()->getRootOperators().empty(), "A pipeline should have at least one root operator");

    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = loweringContext.pipelineToExecutableMap.find(pipeline->getPipelineId());
        executable != loweringContext.pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto executableOperator = NES::Util::as<ExecutableOperator>(rootOperator);
    auto executablePipeline
        = Runtime::Execution::ExecutablePipeline::create(PipelineId(pipeline->getPipelineId()), executableOperator->takeStage(), {});

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

std::unique_ptr<Runtime::Execution::CompiledQueryPlan>
LowerToExecutableQueryPlanPhase::apply(const std::shared_ptr<PipelineQueryPlan>& pipelineQueryPlan)
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
        | std::views::transform(
                     [](auto descriptorAndPredecessors)
                     { return Runtime::Execution::Sink(descriptorAndPredecessors.first, std::move(descriptorAndPredecessors.second)); })
        | std::ranges::to<std::vector>();

    return Runtime::Execution::CompiledQueryPlan::create(
        pipelineQueryPlan->getQueryId(), std::move(pipelines), std::move(sinks), std::move(loweringContext.sources));
}

}
