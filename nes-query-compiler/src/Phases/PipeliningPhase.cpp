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

#include <cstdint>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>
#include <variant>
#include <API/Schema.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/QueryPlan.hpp>
#include <AbstractPhysicalOperator.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

void processFusibleOperator(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(
        std::holds_alternative<std::shared_ptr<AbstractPhysicalOperator>>(currentOperator->op),
        "should only be called with a physical operator");

    currentPipeline->prependOperator(currentOperator);
    currentPipeline->setType(Pipeline::Type::PipelineType); // TODO remove me
    for (const auto& node : currentOperator->children)
    {
        process(pipelinePlan, pipelineOperatorMap, currentPipeline, node);
    }
}

void processPipelineBreakerOperator(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(
        std::holds_alternative<std::shared_ptr<AbstractPhysicalOperator>>(currentOperator->op),
        "should only be called with a physical operator");

    currentPipeline->prependOperator(currentOperator);

    /// We create a new pipeline for each of the current op's childrens
    for (const auto& node : currentOperator->children)
    {
        auto newPipeline = Pipeline::create();
        newPipeline->setType(Pipeline::Type::PipelineType);
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, node);
    }
}

void processSink(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(std::holds_alternative<std::shared_ptr<SinkLogicalOperator>>(currentOperator->op), "should only be called with a sink");

    // goes through all the children of the sink operator. creates a new operator pipeline.
    // The pipeline plan holds a plan with all currently existing pipelines. We add it there.
    // the new pipeline will be added to the current pipeline (source pipelines to sink pipelines)
    // the next operator after the sink gets processed
    for (const auto& child : currentOperator->children)
    {
        auto newPipeline = Pipeline::create();
        newPipeline->setType(Pipeline::Type::SinkPipelineType);
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, child); // TODO should this ever happen?
    }
}

void processSource(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>&,
    std::shared_ptr<Pipeline> currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(
        std::holds_alternative<std::shared_ptr<SourceDescriptorLogicalOperator>>(currentOperator->op),
        "should only be called with a source");

    if (currentPipeline->hasOperators())
    {
        auto newPipeline = Pipeline::create();
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        currentPipeline = newPipeline;
    }
    currentPipeline->setType(Pipeline::Type::SourcePipelineType);
    currentPipeline->prependOperator(currentOperator);
}

void process(
    const std::shared_ptr<PipelinedQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>>& pipelineOperatorMap,
    const std::shared_ptr<Pipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    if (std::holds_alternative<std::shared_ptr<SinkLogicalOperator>>(currentOperator->op))
    {
        processSink(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperator);
    }
    else if (std::holds_alternative<std::shared_ptr<SourceDescriptorLogicalOperator>>(currentOperator->op))
    {
        processSource(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperator);
    }
    else if (true) // TODO we should check here at the physical operator if it is a pipeline breaker
    {
        processFusibleOperator(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperator);
    }
    else
    {
        processPipelineBreakerOperator(pipelinePlan, pipelineOperatorMap, currentPipeline, currentOperator);
    }
}

/// During this step we create a PipelinedQueryPlan out of the QueryPlan obj
std::shared_ptr<PipelinedQueryPlan> apply(const std::unique_ptr<QueryPlan> queryPlan)
{
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<Pipeline>> pipelineOperatorMap;

    auto pipelinePlan = std::make_shared<PipelinedQueryPlan>(queryPlan->getQueryId());

    // Here we get source operators as the root, but we expected sink operators
    for (const auto& sinkOperator : queryPlan->getRootOperators())
    {
        auto pipeline = Pipeline::createSinkPipeline();

        /// Create a new pipeline for each sink
        pipeline->prependOperator(dynamic_cast<SinkLogicalOperator*>(sinkOperator.get()));
        pipelinePlan->pipelines.emplace_back(pipeline);

        /// process next operators
        process(pipelinePlan, pipelineOperatorMap, pipeline, sinkOperator);
    }

    return pipelinePlan;
}

}
