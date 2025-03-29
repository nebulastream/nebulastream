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

#include <Phases/PipeliningPhase.hpp>
#include <memory>
#include <unordered_map>
#include <variant>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/Operator.hpp>
#include <Plans/QueryPlan.hpp>
#include <PhysicalOperator.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

/// forward declaration
void process(
    std::shared_ptr<PipelinedQueryPlan> pipelinePlan,
    std::shared_ptr<Pipeline> currentPipeline,
    std::shared_ptr<Pipeline::PipelineOperator> currentOperator);

void processFusibleOperator(
    std::shared_ptr<PipelinedQueryPlan> pipelinePlan,
    std::shared_ptr<Pipeline> currentPipeline,
    std::shared_ptr<Pipeline::PipelineOperator> currentOperator)
{
    currentPipeline->prependOperator(currentOperator);
    for (const auto& op : NES::Util::as<Operator>(std::get<std::shared_ptr<PhysicalOperator>>(*currentOperator))->children)
    {
        process(pipelinePlan, currentPipeline, NES::Util::as<Pipeline::PipelineOperator>(op));
    }
}

void processPipelineBreakerOperator(
    std::shared_ptr<PipelinedQueryPlan> pipelinePlan,
    std::shared_ptr<Pipeline> currentPipeline,
    std::shared_ptr<Pipeline::PipelineOperator> currentOperator)
{
    currentPipeline->prependOperator(currentOperator);

    /// We create a new pipeline for each of the current op's children
    for (const auto& op : NES::Util::as<Operator>(std::get<std::shared_ptr<PhysicalOperator>>(*currentOperator))->children)
    {
        auto newPipeline = std::make_shared<OperatorPipeline>();
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->successorPipelines.emplace_back(currentPipeline);
        process(pipelinePlan, newPipeline, NES::Util::as<Pipeline::PipelineOperator>(op));
    }
}

void processSink(
    std::shared_ptr<PipelinedQueryPlan> pipelinePlan,
    std::shared_ptr<Pipeline> currentPipeline,
    std::shared_ptr<Pipeline::PipelineOperator> currentOperator)
{
    // goes through all the children of the sink operator. creates a new operator pipeline.
    // The pipeline plan holds a plan with all currently existing pipelines. We add it there.
    // the new pipeline will be added to the current pipeline (source pipelines to sink pipelines)
    // the next operator after the sink gets processed
    for (const auto& op : NES::Util::as<Operator>(std::get<std::shared_ptr<PhysicalOperator>>(*currentOperator))->children)
    {
        auto newPipeline = std::make_shared<SinkPipeline>();
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->successorPipelines.emplace_back(currentPipeline);
        process(pipelinePlan, newPipeline, NES::Util::as<Pipeline::PipelineOperator>(op));
    }
}

void processSource(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::shared_ptr<Pipeline> currentPipeline,
    const std::shared_ptr<Pipeline::PipelineOperator> currentOperator)
{
    /// Source operators will always be part of their own pipeline.
    if (currentPipeline->hasOperators())
    {
        auto newPipeline = std::make_shared<SourcePipeline>();
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->successorPipelines.emplace_back(currentPipeline);
        currentPipeline = newPipeline;
    }
    currentPipeline->prependOperator(currentOperator);
}

void process(
    std::shared_ptr<PipelinedQueryPlan> pipelinePlan,
    std::shared_ptr<Pipeline> currentPipeline,
    std::shared_ptr<Pipeline::PipelineOperator> currentOperator)
{
    if (std::holds_alternative<std::shared_ptr<SourceDescriptorLogicalOperator>>(*currentOperator))
    {
        processSink(pipelinePlan, currentPipeline, currentOperator);
    }
    else if (std::holds_alternative<std::shared_ptr<SinkLogicalOperator>>(*currentOperator))
    {
        processSource(pipelinePlan, currentPipeline, currentOperator);
    }
    else if (auto op = std::get<std::shared_ptr<PhysicalOperator>>(*currentOperator); op->isPipelineBreaker)
    {
        processPipelineBreakerOperator(pipelinePlan, currentPipeline, currentOperator);
    }
    else
    {
        processFusibleOperator(pipelinePlan, currentPipeline, currentOperator);
    }
}

/// During this step we create a PipelinedQueryPlan out of the QueryPlan obj
std::shared_ptr<PipelinedQueryPlan> apply(const std::unique_ptr<QueryPlan> queryPlan)
{
    auto pipelinePlan = std::make_shared<PipelinedQueryPlan>(queryPlan->getQueryId());

    // Here we get source operators as the root, but we expected sink operators
    for (const auto& sourceOperator : queryPlan->getRootOperators())
    {
        /// Create a new pipeline for each source
        auto pipeline = std::make_shared<SourcePipeline>();
        pipeline->prependOperator(Util::as<Pipeline::PipelineOperator>(sourceOperator));
        pipelinePlan->pipelines.emplace_back(pipeline);

        process(pipelinePlan, pipeline, NES::Util::as<Pipeline::PipelineOperator>(sourceOperator));
    }
    return pipelinePlan;
}

}
