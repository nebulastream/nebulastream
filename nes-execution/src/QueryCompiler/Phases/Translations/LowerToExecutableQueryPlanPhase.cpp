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
#include <InputFormatters/InputFormatterOperatorHandler.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sinks/Sink.hpp>
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
    PipelineId::Underlying pipelineIdGenerator = PipelineId::INITIAL;
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

std::shared_ptr<Runtime::Execution::ExecutablePipeline> processInputFormatter(
    PipelineId pipelineId,
    std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>> successorPipelinesOfInputFormatter,
    const Sources::SourceDescriptor& sourceDescriptor)
{
    // Todo: how does the input formatter task get access to its operator handler?
    // - should be in the pipeline execution context
    //      - setOperatorHandlers()
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers{std::make_shared<InputFormatters::InputFormatterProvider::InputFormatterOperatorHandler>()};

    auto inputFormatterTask = NES::InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceDescriptor.parserConfig.parserType,
        sourceDescriptor.schema,
        sourceDescriptor.parserConfig.tupleDelimiter,
        sourceDescriptor.parserConfig.fieldDelimiter);
    auto executableInputFormatterPipeline = Runtime::Execution::ExecutablePipeline::create(
        pipelineId, std::move(inputFormatterTask), successorPipelinesOfInputFormatter);
    return executableInputFormatterPipeline;
}

Runtime::Execution::Source
processSource(const OperatorPipelinePtr& pipeline, const PipelineQueryPlanPtr& pipelineQueryPlan, LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->getDecomposedQueryPlan()->toString());

    /// Convert logical source descriptor to actual source descriptor
    const auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    const auto sourceOperator = NES::Util::as<SourceDescriptorLogicalOperator>(rootOperator);

    std::vector<std::shared_ptr<Runtime::Execution::ExecutablePipeline>> executableSuccessorPipelines;
    auto inputFormatterTask = NES::InputFormatters::InputFormatterProvider::provideInputFormatter(
        sourceOperator->getSourceDescriptorRef().parserConfig.parserType,
        sourceOperator->getSourceDescriptorRef().schema,
        sourceOperator->getSourceDescriptorRef().parserConfig.tupleDelimiter,
        sourceOperator->getSourceDescriptorRef().parserConfig.fieldDelimiter);
    for (const auto& successor : pipeline->getSuccessors())
    {
        if (auto executableSuccessor = processSuccessor(sourceOperator->getOriginId(), successor, pipelineQueryPlan, loweringContext))
        {
            executableSuccessorPipelines.emplace_back(*executableSuccessor);
        }
    }
    // Todo: the weak ptr will probably be destroyed
    // - might need to change from weak to shared_ptr
    std::vector<std::weak_ptr<Runtime::Execution::ExecutablePipeline>> inputFormatterTasks;
    auto executableInputFormatterPipeline = Runtime::Execution::ExecutablePipeline::create(
        pipeline->getPipelineId(), std::move(inputFormatterTask), executableSuccessorPipelines);
    inputFormatterTasks.emplace_back(std::move(executableInputFormatterPipeline));

    loweringContext.sources.emplace_back(sourceOperator->getOriginId(), sourceOperator->getSourceDescriptor(), inputFormatterTasks);
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
    PRECONDITION(!pipeline->getDecomposedQueryPlan()->getRootOperators().empty(), "A pipeline should have at least one root operator");

    /// check if the particular pipeline already exist in the pipeline map.
    if (const auto executable = loweringContext.pipelineToExecutableMap.find(pipeline->getPipelineId());
        executable != loweringContext.pipelineToExecutableMap.end())
    {
        return executable->second;
    }
    auto rootOperator = pipeline->getDecomposedQueryPlan()->getRootOperators()[0];
    auto executableOperator = NES::Util::as<ExecutableOperator>(rootOperator);
    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(
        PipelineId(loweringContext.pipelineIdGenerator++), executableOperator->takeStage(), {});

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

std::unique_ptr<Runtime::Execution::ExecutableQueryPlan>
LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan)
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

    return Runtime::Execution::ExecutableQueryPlan::create(
        pipelineQueryPlan->getQueryId(), std::move(pipelines), std::move(sinks), std::move(loweringContext.sources));
}

}
