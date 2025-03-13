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
#include <Util/Common.hpp>
#include <ExecutableOperator.hpp>
#include <ExecutableQueryPlan.hpp>
#include <PipelinedQueryPlan.hpp>
#include <Pipeline.hpp>
#include <ErrorHandling.hpp>
#include <Phases/LowerToExecutableQueryPlanPhase.hpp>

namespace NES::QueryCompilation::LowerToExecutableQueryPlanPhase
{

class LoweringContext {
public:
    struct PredecessorRef {
        std::variant<OriginId, std::weak_ptr<ExecutablePipeline>> ref;
        explicit PredecessorRef(const OriginId& id) : ref(id) {}
        explicit PredecessorRef(const std::weak_ptr<ExecutablePipeline>& ptr) : ref(ptr) {}
    };

    void addSink(const std::shared_ptr<Sinks::SinkDescriptor>& sinkDesc, const PredecessorRef& predecessor) {
        sinks[sinkDesc].push_back(predecessor);
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
            result.emplace_back(sinkDescriptor, preds);
        }
        return result;
    }

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
    PipelinedQueryPlan pipelineQueryPlan,
    LoweringContext& loweringContext);


std::shared_ptr<ExecutablePipeline> processPipeline(
    std::unique_ptr<Pipeline> pipeline,
    const PipelinedQueryPlan& pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION(pipeline->hasOperators(), "A pipeline should have at least one root operator");

    if (auto execPipelineOpt = loweringContext.getPipeline(pipeline->pipelineId)) {
        return execPipelineOpt.value();
    }

    auto* opPipeline = dynamic_cast<OperatorPipeline*>(pipeline.get());
    PRECONDITION(opPipeline != nullptr, "expected an OperatorPipeline {}", pipeline->toString());

    PRECONDITION(opPipeline->rootOperator != nullptr, "OperatorPipeline must have a valid rootOperator");
    auto& physicalOperator = opPipeline->rootOperator;

    auto executablePipeline = ExecutablePipeline::create(
        PipelineId(pipeline->pipelineId),
        physicalOperator->takeStage(),
        {});

    LoweringContext::PredecessorRef predecessorRef(executablePipeline);
    for (auto& successor : pipeline->successorPipelines)
    {
        if (auto executableSuccessor = processSuccessor(predecessorRef, std::move(successor), pipelineQueryPlan, loweringContext))
        {
            executablePipeline->successors.emplace_back(*executableSuccessor);
        }
    }

    loweringContext.registerPipeline(pipeline->pipelineId, executablePipeline);
    return executablePipeline;
}

Source processSource(std::unique_ptr<Pipeline> pipeline, const PipelinedQueryPlan& pipelineQueryPlan, LoweringContext& loweringContext)
{
    PRECONDITION((Util::uniquePtrInstanceOf<Pipeline, SourcePipeline>(pipeline)),
                 "expected a SourcePipeline {}", pipeline->toString());
    auto sourcePipeline = NES::Util::unique_ptr_dynamic_cast<SourcePipeline>(std::move(pipeline));

    std::vector<std::weak_ptr<ExecutablePipeline>> executableSuccessorPipelines;
    for (auto& successor : sourcePipeline->successorPipelines)
    {
        if (auto executableSuccessor = processSuccessor(sourcePipeline->sourceOperator->getOriginId(),
                                                        std::move(successor),
                                                        pipelineQueryPlan,
                                                        loweringContext))
        {
            executableSuccessorPipelines.push_back(*executableSuccessor);
        }
    }

    Source newSource(sourcePipeline->sourceOperator->getOriginId(),
                     std::make_shared<NES::Sources::SourceDescriptor>(sourcePipeline->sourceOperator->getSourceDescriptor()),
                     executableSuccessorPipelines);
    loweringContext.addSource(newSource);
    return newSource;
}


void processSink(const OriginId& predecessor, std::unique_ptr<Pipeline> pipeline, LoweringContext& loweringContext)
{
    PRECONDITION((Util::uniquePtrInstanceOf<Pipeline, SinkPipeline>(pipeline)),
                 "expected a SinkPipeline {}", pipeline->toString());
    auto sinkPipeline = NES::Util::unique_ptr_dynamic_cast<SinkPipeline>(std::move(pipeline));
    auto& sinkOperatorDescriptor = sinkPipeline->sinkOperator->sinkDescriptor;
    loweringContext.addSink(sinkOperatorDescriptor, LoweringContext::PredecessorRef(predecessor));
}

std::optional<std::shared_ptr<ExecutablePipeline>> processSuccessor(
    const OriginId& predecessor,
    std::unique_ptr<Pipeline> pipeline,
    PipelinedQueryPlan pipelineQueryPlan,
    LoweringContext& loweringContext)
{
    PRECONDITION((Util::uniquePtrInstanceOf<Pipeline, SinkPipeline>(pipeline) ||
                  Util::uniquePtrInstanceOf<Pipeline, OperatorPipeline>(pipeline)),
                 "expected a Sink or Pipeline {}", pipeline->toString());

    if (Util::uniquePtrInstanceOf<Pipeline, SinkPipeline>(pipeline))
    {
        processSink(predecessor, std::move(pipeline), loweringContext);
        return std::nullopt;
    }
    return processPipeline(std::move(pipeline), pipelineQueryPlan, loweringContext);
}


std::unique_ptr<CompiledQueryPlan> LowerToExecutableQueryPlanPhase::apply(const std::unique_ptr<PipelinedQueryPlan> pipelineQueryPlan)
{
    LoweringContext loweringContext;

    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (auto& pipeline : sourcePipelines)
    {
        processSource(std::move(pipeline), pipelineQueryPlan, loweringContext);
    }

    auto pipelines = loweringContext.getPipelines();
    auto sinks = loweringContext.getSinks();
    return std::make_unique<CompiledQueryPlan>(pipelineQueryPlan->queryId, std::move(pipelines), std::move(sinks), loweringContext.getSources());
}

}
