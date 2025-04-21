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
#include <Nautilus/Interface/MemoryProvider/RowTupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/QueryPlan.hpp>
#include <Plans/PipelineQueryPlan.hpp>
#include <Emit.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <Scan.hpp>

namespace NES::QueryCompilation::PipeliningPhase
{

// forward ref for now
void process(
    const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
    const std::shared_ptr<OperatorPipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator);

void processFusibleOperator(
    const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
    const std::shared_ptr<OperatorPipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(
        std::holds_alternative<std::shared_ptr<PhysicalOperator>>(currentOperator->op), "should only be called with a physical operator");

    currentPipeline->prependOperator(currentOperator);
    currentPipeline->setType(OperatorPipeline::Type::OperatorPipelineType); // TODO remove me
    for (const auto& node : currentOperator->children)
    {
        process(pipelinePlan, pipelineOperatorMap, currentPipeline, node);
    }
}

void processPipelineBreakerOperator(
    const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
    const std::shared_ptr<OperatorPipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(
        std::holds_alternative<std::shared_ptr<PhysicalOperator>>(currentOperator->op), "should only be called with a physical operator");

    currentPipeline->prependOperator(currentOperator);

    /// We create a new pipeline for each of the current op's childrens
    for (const auto& node : currentOperator->children)
    {
        auto newPipeline = OperatorPipeline::create();
        newPipeline->setType(OperatorPipeline::Type::OperatorPipelineType);
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, node);
    }
}

void processSink(
    const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
    const std::shared_ptr<OperatorPipeline>& currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(std::holds_alternative<std::shared_ptr<SinkLogicalOperator>>(currentOperator->op), "should only be called with a sink");

    // goes through all the children of the sink operator. creates a new operator pipeline.
    // The pipeline plan holds a plan with all currently existing pipelines. We add it there.
    // the new pipeline will be added to the current pipeline (source pipelines to sink pipelines)
    // the next operator after the sink gets processed
    for (const auto& child : currentOperator->children)
    {
        auto newPipeline = OperatorPipeline::create();
        newPipeline->setType(OperatorPipeline::Type::SinkPipelineType);
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        process(pipelinePlan, pipelineOperatorMap, newPipeline, child); // TODO should this ever happen?
    }
}

void processSource(
    const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>&,
    std::shared_ptr<OperatorPipeline> currentPipeline,
    const std::shared_ptr<PhysicalOperatorNode>& currentOperator)
{
    PRECONDITION(
        std::holds_alternative<std::shared_ptr<SourceDescriptorLogicalOperator>>(currentOperator->op),
        "should only be called with a source");

    if (currentPipeline->hasOperators())
    {
        auto newPipeline = OperatorPipeline::create();
        pipelinePlan->pipelines.emplace_back(newPipeline);
        newPipeline->addSuccessor(currentPipeline);
        currentPipeline = newPipeline;
    }
    currentPipeline->setType(OperatorPipeline::Type::SourcePipelineType);
    currentPipeline->prependOperator(currentOperator);
}

void process(
    const std::shared_ptr<PipelineQueryPlan>& pipelinePlan,
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>>& pipelineOperatorMap,
    const std::shared_ptr<OperatorPipeline>& currentPipeline,
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

/// During this step we create a PipelineQueryPlan out of the QueryPlan obj
std::shared_ptr<PipelineQueryPlan> apply(const std::unique_ptr<QueryPlan> queryPlan)
{
    std::unordered_map<std::shared_ptr<Operator>, std::shared_ptr<OperatorPipeline>> pipelineOperatorMap;
    auto pipelinePlan = std::make_shared<PipelineQueryPlan>(queryPlan->getQueryId()); // TODO can we make this unique?

    // Here we get source operators as the root, but we expected sink operators
    for (const auto& sinkOperator : queryPlan->getRootOperators())
    {
        INVARIANT(dynamic_cast<SinkLogicalOperator*>(sinkOperator.get()),
                  "We expect that all root operators in a physical query plan are sink operators");
        //INVARIANT(
        //    std::holds_alternative<std::shared_ptr<SinkLogicalOperator>>(sinkOperator->op),
        //    "We expect that all root operators in a physical query plan are sink operators");

        /// create a new pipeline for each sink
        auto pipeline = OperatorPipeline::createSinkPipeline();

        pipeline->prependOperator(
            std::make_shared<PhysicalOperatorNode>(dynamic_cast<SinkLogicalOperator*>(sinkOperator.get())));
        pipelinePlan->pipelines.emplace_back(pipeline);

        /// process next operators
        process(pipelinePlan, pipelineOperatorMap, pipeline, sinkOperator);
    }

    for (const auto& pipeline : pipelinePlan->pipelines)
    {
        std::cout << pipeline->toString() << "\n";
    }

    return pipelinePlan;
}

}
