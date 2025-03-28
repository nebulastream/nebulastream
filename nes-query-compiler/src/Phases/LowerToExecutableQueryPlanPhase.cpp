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
#include <Sinks/SinkDescriptor.hpp>
#include <ExecutableQueryPlan.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Pipeline.hpp>
#include <ErrorHandling.hpp>
#include <Phases/LowerToExecutableQueryPlanPhase.hpp>
#include <ExecutablePipelineProviderRegistry.hpp>
#include <options.hpp>
#include <CompiledQueryPlan.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES::QueryCompilation::LowerToExecutableQueryPlanPhase
{

class LoweringContext {
public:
    struct PredecessorRef {
        std::variant<OriginId, std::weak_ptr<ExecutablePipeline>> ref;
        explicit PredecessorRef(const OriginId& id) : ref(id) {}
        explicit PredecessorRef(const std::weak_ptr<ExecutablePipeline>& ptr) : ref(ptr) {}
    };

    void addSink(std::unique_ptr<Sinks::SinkDescriptor> sinkDesc, const PredecessorRef& predecessor) {
        auto sinkDescShared = std::shared_ptr<Sinks::SinkDescriptor>(std::move(sinkDesc));
        sinks[sinkDescShared].push_back(predecessor);
    }


    void addSource(const Source& source) {
        sources.push_back(source);
    }

    void registerPipeline(PipelineId id, const std::shared_ptr<ExecutablePipeline>& pipeline) {
        pipelineToExecutableMap[id] = pipeline;
    }

    [[nodiscard]] std::optional<std::shared_ptr<ExecutablePipeline>> getPipeline(PipelineId id) const {
        auto it = pipelineToExecutableMap.find(id);
        if (it != pipelineToExecutableMap.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    [[nodiscard]] const std::vector<Source>& getSources() const { return sources; }

    [[nodiscard]] std::vector<std::shared_ptr<ExecutablePipeline>> getPipelines() const {
        std::vector<std::shared_ptr<ExecutablePipeline>> result;
        for (const auto& [id, pipeline] : pipelineToExecutableMap) {
            result.push_back(pipeline);
        }
        return result;
    }

    [[nodiscard]] std::vector<Sink> getSinks() const {
        std::vector<Sink> result;
        for (const auto& [sinkDescriptor, preds] : sinks) {
            std::vector<std::variant<OriginId, std::weak_ptr<ExecutablePipeline>>> convertedPreds;
            convertedPreds.reserve(preds.size());
            for (const auto& predRef : preds) {
                convertedPreds.push_back(predRef.ref);
            }
            result.emplace_back(sinkDescriptor, convertedPreds);
        }
        return result;
    }

    nautilus::engine::Options options;

private:
    std::unordered_map<
        std::shared_ptr<Sinks::SinkDescriptor>,
        std::vector<PredecessorRef>> sinks;
    std::vector<Source> sources;
    std::unordered_map<PipelineId, std::shared_ptr<ExecutablePipeline>> pipelineToExecutableMap;
};

/// Forward decl.
std::optional<std::shared_ptr<ExecutablePipeline>> processSuccessor(
    const OriginId& predecessor,
    std::unique_ptr<Pipeline> pipeline,
    const PipelinedQueryPlan& pipelineQueryPlan,
    LoweringContext& loweringContext);


std::shared_ptr<ExecutablePipeline> processPipeline(
    std::unique_ptr<Pipeline> pipeline,
    const PipelinedQueryPlan& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline != nullptr, "Expected an OperatorPipeline");

    loweringContext.options.setOption("toConsole", true);
    loweringContext.options.setOption("toFile", true);

    auto providerArguments = ExecutablePipelineProviderRegistryArguments{};
    if (const auto provider = ExecutablePipelineProviderRegistry::instance().create(pipeline->getProviderType(), providerArguments))
    {
        auto pipelineStage = provider.value()->create(std::move(pipeline), pipeline->releaseOperatorHandlers(), loweringContext.options);
        auto executablePipeline = ExecutablePipeline::create(
            PipelineId(pipeline->pipelineId),
            std::move(pipelineStage),
            {});

        const LoweringContext::PredecessorRef predecessorRef(executablePipeline);
        for (auto& successor : pipeline->successorPipelines)
        {
            const OriginId& origin = std::get<OriginId>(predecessorRef.ref);
            if (auto executableSuccessor = processSuccessor(origin, std::move(successor), pipelineQueryPlan, loweringContext))
            {
                executablePipeline->successors.emplace_back(*executableSuccessor);
            }
        }

        loweringContext.registerPipeline(pipeline->pipelineId, executablePipeline);
        return executablePipeline;
    }
    throw UnknownExecutablePipelineProviderType("ExecutablePipelineProvider plugin of type: {} not registered.", pipeline->getProviderType());
}

Source processSource(std::unique_ptr<Pipeline> pipeline, const PipelinedQueryPlan& pipelineQueryPlan, LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSourcePipeline(), "expected a SourcePipeline {}", pipeline->toString());

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (auto& successor : pipeline->successorPipelines)
    {
        if (auto executableSuccessor = processSuccessor(pipeline->getOperator<SourceDescriptorLogicalOperator>().value()->originIdTrait.originIds[0],
                                                        std::move(successor),
                                                        pipelineQueryPlan,
                                                        loweringContext))
        {
            executableSuccessorPipelines.push_back(*executableSuccessor);
        }
    }

    Source newSource(pipeline->getOperator<SourceDescriptorLogicalOperator>().value()->originIdTrait.originIds[0],
                     std::make_shared<NES::Sources::SourceDescriptor>(pipeline->getOperator<SourceDescriptorLogicalOperator>().value()->getSourceDescriptor()),
                     executableSuccessorPipelines);
    loweringContext.addSource(newSource);
    return newSource;
}


void processSink(const OriginId& predecessor, std::unique_ptr<Pipeline> pipeline, LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSinkPipeline(), "expected a SinkPipeline {}", pipeline->toString());
    auto& sinkOperatorDescriptor = pipeline->getOperator<SinkLogicalOperator>().value()->sinkDescriptor;
    loweringContext.addSink(std::move(sinkOperatorDescriptor), LoweringContext::PredecessorRef(predecessor));
}

std::optional<std::shared_ptr<ExecutablePipeline>> processSuccessor(
    const OriginId& predecessor,
    std::unique_ptr<Pipeline> pipeline,
    const PipelinedQueryPlan& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->isSinkPipeline() or pipeline->isOperatorPipeline(),
                 "expected a Sink or Pipeline {}", pipeline->toString());

    if (pipeline->isSinkPipeline())
    {
        processSink(predecessor, std::move(pipeline), loweringContext);
        return std::nullopt;
    }
    return processPipeline(std::move(pipeline), pipelineQueryPlan, loweringContext);
}


std::unique_ptr<CompiledQueryPlan> apply(std::unique_ptr<PipelinedQueryPlan> pipelineQueryPlan)
{
    LoweringContext loweringContext;
    for (auto& pipeline : pipelineQueryPlan->releaseSourcePipelines())
    {
        processSource(std::move(pipeline), *pipelineQueryPlan, loweringContext);
    }

    auto pipelines = loweringContext.getPipelines();
    auto sinks = loweringContext.getSinks();
    return std::make_unique<CompiledQueryPlan>(pipelineQueryPlan->queryId, std::move(pipelines), std::move(sinks), loweringContext.getSources());
}

}
